package utils;

import exception.DeleteZNodeDataErrorException;
import exception.WriteZNodeDataErrorException;
import jobs.FSVisitor;
import jobs.FSWalker;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static utils.BDRConstant.BROKERS_ROOT_IN_ZOOKEEPER;

public class CommonUtils {
    public static void cleanOldDataInZK(ZooKeeper zooKeeper, String znode) throws Exception {
        if (zooKeeper.exists(znode, false) != null && !deleteRecursive(zooKeeper, znode)) {
            System.out.println("Error: Failed to delete kafka old data, please clear manually");
            throw new DeleteZNodeDataErrorException("Clean old data failed");
        }
        System.out.println("Info: Delete zookeeper success.");
    }

    public static boolean deleteRecursive(ZooKeeper zooKeeper, String path) {
        List<String> children;
        try {
            children = zooKeeper.getChildren(path, false);
        } catch (KeeperException.NoNodeException e) {
            return true;
        } catch (Exception e) {
            return false;
        }

        for (String subPath : children) {
            if (!deleteRecursive(zooKeeper, path + "/" + subPath)) {
                return false;
            }
        }
        return delete(zooKeeper, path, -1);
    }

    public static boolean delete(ZooKeeper zooKeeper, String path, int version) {
        int retryNum = 3;
        boolean retryFlag = true;
        while (retryNum-- > 0 && retryFlag) {
            try {
                zooKeeper.delete(path, version);
                retryFlag = false;
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException se) {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    public static String writeDataToZookeeper(String taskType, String inputDir, FSVisitor visitor) {
        System.out.println("Info: +" + taskType + "Task start,visitor is " + visitor.getClass().getName());
        System.out.println("Info: InputDir is " + inputDir);
        File inputFile = new File(inputDir);
        BDRFileFilter filter = new BDRFileFilter("Brokers".equals(taskType) ? "topics" : "brokers");
        File[] children = inputFile.listFiles(filter);
        if (null == children) {
            String errMsg = "Error: InputDir " + inputDir + "in " + taskType + "Task is invalid";
            System.out.println(errMsg);
            throw new WriteZNodeDataErrorException(errMsg);
        }
        for (File child : children) {
            if (child.isDirectory()) {
                String zookeeperRoot = "/" + System.getProperty("zookeeperRoot");
                String currPath = inputDir + File.separator + child.getName();
                String znode = "Brokers".equals(taskType) ? (zookeeperRoot + BROKERS_ROOT_IN_ZOOKEEPER) : (zookeeperRoot + "/" + child.getName());
                FSWalker walker = new FSWalker(currPath, znode);
                try {
                    walker.go(visitor);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new WriteZNodeDataErrorException(e);
                }
            }
        }
        System.out.println("Info: task.BrokersTask end. " + Thread.currentThread().getName());
        return Thread.currentThread().getName();
    }

    public static void shutDownThreadPool(ExecutorService jobExecutor) throws InterruptedException {
        jobExecutor.shutdown();
        try {
            if (!jobExecutor.awaitTermination(60L, TimeUnit.MINUTES)) {
                System.out.println("Error: Some task are timed out, shut down now.");
                jobExecutor.shutdownNow();
            } else {
                System.out.println("Info: job.Job is complete.");
            }
        } catch (InterruptedException ie) {
            System.out.println("Error: InterruptedException found, job failed.");
            jobExecutor.shutdownNow();
            throw ie;
        }
    }

    public static void closeZookeeperConnection(ZooKeeper zooKeeper) {
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
            }
        } catch (InterruptedException e) {
            System.out.println("Warn: Close zookeeper error.");
        }
    }
}
