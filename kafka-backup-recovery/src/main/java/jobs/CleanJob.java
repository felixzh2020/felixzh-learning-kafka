package jobs;

import exception.CleanZookeeperDataException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import task.BrokersTopicCleanTask;
import task.ConfigTopicCleanTask;
import utils.CommonUtils;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static utils.BDRConstant.CONFIG_TOPICS_ROOT_IN_ZOOKEEPER;
import static utils.BDRConstant.TOPICS_ROOT_IN_ZOOKEEPER;

public class CleanJob implements Job, Watcher {
    private final String zkServer;

    private final String father;

    private ZooKeeper zk;

    private ExecutorService jobExecutor = null;

    public CleanJob(String zkServer, String znode) {
        this.zkServer = zkServer;
        this.father = znode;
        this.jobExecutor = Executors.newFixedThreadPool(10);
    }

    @Override
    public void go() {
        System.out.println("Info: Running clean job: ");
        System.out.println("Info: Zookeeper server: " + this.zkServer);
        System.out.println("Info: Clean zookeeper path: " + this.father);
        try {
            String zookeeperRoot = "/" + System.getProperty("zookeeperRoot");
            this.zk = createZookeeperConnection(this.zkServer, this.father, 60000);
            cleanBrokersTopicTask(this.jobExecutor, this.zk, zookeeperRoot + TOPICS_ROOT_IN_ZOOKEEPER);
            cleanConfigTopicTask(this.jobExecutor, this.zk, zookeeperRoot + CONFIG_TOPICS_ROOT_IN_ZOOKEEPER);
            CommonUtils.shutDownThreadPool(this.jobExecutor);
            CommonUtils.cleanOldDataInZK(this.zk, zookeeperRoot);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new CleanZookeeperDataException("Something wrong during clean zookeeper path.", e);
        } finally {
            CommonUtils.closeZookeeperConnection(this.zk);
        }
    }

    private ZooKeeper createZookeeperConnection(String zkServer, String father, int timeout) throws IOException, InterruptedException {
        ZooKeeper zooKeeper = new ZooKeeper(zkServer + father, timeout, this);
        zooKeeper.addAuthInfo("krbgroup", null);
        long startTime = System.currentTimeMillis();
        while (!zooKeeper.getState().isConnected()) {
            System.out.println("Warn: Re-connecting to " + zkServer + " with chroot " + father);
            Thread.sleep(1000L);
            long currentTime = System.currentTimeMillis();
            long costTime = currentTime - startTime;
            if (costTime > timeout) {
                throw new IOException("Connecting to ZooKeeper failed.");
            }

        }
        System.out.println("Info: Connect zookeeper done.");
        return zooKeeper;
    }

    private void cleanBrokersTopicTask(ExecutorService jobExecutor, ZooKeeper zooKeeper, String znode) throws Exception {
        BrokersTopicCleanTask brokersTopicCleanTask = new BrokersTopicCleanTask(zooKeeper, znode);
        Future<String> future = jobExecutor.submit(brokersTopicCleanTask);
        System.out.println(future.isDone());
    }

    private void cleanConfigTopicTask(ExecutorService jobExecutor, ZooKeeper zooKeeper, String znode) throws Exception {
        ConfigTopicCleanTask configTopicCleanTask = new ConfigTopicCleanTask(zooKeeper, znode);
        Future<String> future = jobExecutor.submit(configTopicCleanTask);
        System.out.println(future.isDone());
    }

    @Override
    public void process(WatchedEvent watchedEvent) {

    }
}
