package task;

import exception.DeleteZNodeDataErrorException;
import org.apache.zookeeper.ZooKeeper;
import utils.CommonUtils;

import java.util.concurrent.Callable;

public class BrokersTopicCleanTask implements Callable<String> {

    private final ZooKeeper zooKeeper;

    private final String znode;

    public BrokersTopicCleanTask(ZooKeeper zooKeeper, String znode) {
        this.zooKeeper = zooKeeper;
        this.znode = znode;
    }

    @Override
    public String call() throws Exception {
        System.out.println("Info: BrokerTopicCleanTask start...");
        System.out.println("Info: CleanDir is " + this.znode);
        if (this.zooKeeper.exists(this.znode, false) != null && !CommonUtils.deleteRecursive(this.zooKeeper, this.znode)) {
            System.out.println("Error: Failed to delete kafka brokers topic data, please clear manually.");
            throw new DeleteZNodeDataErrorException("Clean kafka brokers topic data failed.");
        }
        System.out.println("Info: Clean kafka brokers topic data success.");
        return Thread.currentThread().getName();
    }
}
