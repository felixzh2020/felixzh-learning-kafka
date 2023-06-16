package task;

import exception.DeleteZNodeDataErrorException;
import org.apache.zookeeper.ZooKeeper;
import utils.CommonUtils;

import java.util.concurrent.Callable;

public class ConfigTopicCleanTask implements Callable<String> {

    private final ZooKeeper zooKeeper;
    private final String znode;

    public ConfigTopicCleanTask(ZooKeeper zooKeeper, String znode) {
        this.zooKeeper = zooKeeper;
        this.znode = znode;
    }

    @Override
    public String call() throws Exception {
        System.out.println("Info: task.ConfigTopicCleanTask start...");
        System.out.println("Info: CleanDir is " + this.znode);
        if (this.zooKeeper.exists(this.znode, false) != null && !CommonUtils.deleteRecursive(this.zooKeeper, this.znode)) {
            System.out.println("Error: Failed to delete kafka config topic data, please clear manually.");
            throw new DeleteZNodeDataErrorException("Clean kafka config topic data failed.");
        }
        System.out.println("Info: Clean kafka config topic data success.");
        return Thread.currentThread().getName();
    }
}
