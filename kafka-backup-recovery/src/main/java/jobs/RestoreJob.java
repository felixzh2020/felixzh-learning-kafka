package jobs;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import task.BrokersTask;
import task.KafkaTask;
import task.TopicTask;
import utils.CommonUtils;
import utils.FileSizeCompare;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static utils.BDRConstant.BROKERS_ROOT_IN_ZOOKEEPER;
import static utils.BDRConstant.TOPICS_ROOT_IN_ZOOKEEPER;

public class RestoreJob implements Job, Watcher, FSVisitor {

    private final String zkServer;
    private final String father;
    private final String inputDir;
    private ZooKeeper zk;

    private ExecutorService jobExecutor = null;

    public RestoreJob(String zkServer, String znode, String inputDir) {
        this.zkServer = zkServer;
        this.father = znode;
        this.inputDir = inputDir;
        this.jobExecutor = Executors.newFixedThreadPool(10);
    }

    @Override
    public void go() {
        System.out.println("Info: Running restore job: ");
        System.out.println("Info: ZooKeeper server: " + this.zkServer);
        System.out.println("Info: Reading from local directory: " + this.inputDir);
        System.out.println("Info: Restoring to zookeeper path: " + this.father);

        try {
            String zookeeperRoot = "/" + System.getProperty("zookeeperRoot");
            this.zk = createZookeeperConnection(this.zkServer, this.father);
            CommonUtils.cleanOldDataInZK(this.zk, this.father);
            submitKafkaTask(this.jobExecutor, this.inputDir, zookeeperRoot);
            submitBrokersTask(this.jobExecutor, this.inputDir + BROKERS_ROOT_IN_ZOOKEEPER, zookeeperRoot + BROKERS_ROOT_IN_ZOOKEEPER);
            submitTopicTask(this.jobExecutor, this.inputDir + TOPICS_ROOT_IN_ZOOKEEPER, zookeeperRoot + TOPICS_ROOT_IN_ZOOKEEPER);
            CommonUtils.shutDownThreadPool(this.jobExecutor);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        } finally {
            CommonUtils.closeZookeeperConnection(this.zk);
        }
    }

    private ZooKeeper createZookeeperConnection(String zkServer, String father) throws IOException, InterruptedException {
        ZooKeeper zooKeeper = new ZooKeeper(zkServer + father, 60_000, this);
        zooKeeper.addAuthInfo("krbgroup", null);
        while (!zooKeeper.getState().isConnected()) {
            System.out.println("Warn: Re-Connecting to " + zkServer + " with chroot " + father);
            Thread.sleep(1_000L);
        }
        System.out.println("Info: Connect zookeeper done.");
        return zooKeeper;
    }

    private void submitKafkaTask(ExecutorService jobExecutor, String inputPath, String znode) throws Exception {
        File kafkaFile = new File(inputPath);
        FSWalker.createOne(this, kafkaFile, znode);
        KafkaTask kafkaTask = new KafkaTask(this, inputPath);
        Future<String> future = jobExecutor.submit(kafkaTask);
        System.out.println(future.isDone());
    }

    private void submitBrokersTask(ExecutorService jobExecutor, String inputPath, String znode) throws Exception {
        File brokerFile = new File(inputPath);
        FSWalker.createOne(this, brokerFile, znode);
        BrokersTask brokerTask = new BrokersTask(this, inputPath);
        Future<String> future = jobExecutor.submit(brokerTask);
        System.out.println(future.isDone());
    }

    private void submitTopicTask(ExecutorService jobExecutor, String inputPath, String znode) throws Exception {
        File topicFile = new File(inputPath);
        FSWalker.createOne(this, topicFile, znode);
        File[] sortedFiles = topicFile.listFiles();
        FileSizeCompare fileSizeCompare = new FileSizeCompare();
        sortedFiles = fileSizeCompare.sortFile(sortedFiles);
        for (File file : sortedFiles) {
            if (file.isDirectory()) {
                TopicTask topicTask = new TopicTask(this, inputPath + "/" + file.getName());
                Future<String> future = jobExecutor.submit(topicTask);
                System.out.println(future.isDone());
            }
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
    }

    @Override
    public void visit(File paramFile, byte[] data, List<ACL> acl, String znode) throws Exception {
        createOrSetZnode(data, acl, znode);
    }

    private void createOrSetZnode(byte[] data, List<ACL> acl, String znode) throws Exception {
        int retryNum = 3;
        boolean retryFlag = true;

        while (retryNum-- > 0 && retryFlag) {
            try {
                if (null == acl || acl.isEmpty()) {
                    this.zk.create(znode, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } else {
                    this.zk.create(znode, data, acl, CreateMode.PERSISTENT);
                }
                retryFlag = false;
            } catch (KeeperException.SessionExpiredException se) {
                System.out.println("Error: SessionExpiredException happened when create znode " + znode + ", retry " + (3 - retryNum));
                Thread.sleep(1000L);
            } catch (KeeperException.ConnectionLossException ce) {
                System.out.println("Error: ConnectionLossException happened when create znode " + znode + ", retry " + (3 - retryNum));
                Thread.sleep(1000L);
            } catch (KeeperException.NoNodeException e) {
                System.out.println("Error: NoNodeException happened when create znode " + znode + ", restoreJob will exit.");
                throw e;
            } catch (KeeperException.NodeExistsException e) {
                try {
                    if (null != acl && !acl.isEmpty()) {
                        this.zk.setACL(znode, acl, 0);
                    } else {
                        byte[] zdata = this.zk.getData(znode, false, null);
                        if (!Arrays.equals(data, zdata)) {
                            this.zk.setData(znode, data, -1);
                        }
                    }
                    retryFlag = false;
                } catch (KeeperException.SessionExpiredException se) {
                    System.out.println("Error: SessionExpiredException happened when setACL or setData, retry " + (3 - retryNum));
                    Thread.sleep(1000L);
                } catch (KeeperException.ConnectionLossException ce) {
                    System.out.println("Error: ConnectionLossException happened when setACL or setData, retry " + (3 - retryNum));
                    Thread.sleep(1000L);
                } catch (Exception e2) {
                    System.out.println("Error: Set data or set ACL failed, znode is " + znode);
                    throw e2;
                }
            }
        }
    }
}
