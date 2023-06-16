package task;

import exception.WriteZNodeDataErrorException;
import jobs.FSVisitor;
import jobs.FSWalker;

import java.io.File;
import java.util.concurrent.Callable;

import static utils.BDRConstant.TOPICS_ROOT_IN_ZOOKEEPER;

public class TopicTask implements Callable<String> {

    private final FSVisitor visitor;
    private final String inputDir;

    public TopicTask(FSVisitor visitor, String inputDir) {
        this.visitor = visitor;
        this.inputDir = inputDir;
    }

    @Override
    public String call() {
        System.out.println("Info: task.TopicTask start,visitor is " + this.visitor.getClass().getName());
        System.out.println("Info: inputDir is " + this.inputDir);
        File inputFile = new File(this.inputDir);
        if (inputFile.isDirectory()) {
            String znode = "/" + System.getProperty("zookeeperRoor") + TOPICS_ROOT_IN_ZOOKEEPER + "/" + inputFile.getName();
            FSWalker walker = new FSWalker(this.inputDir, znode);
            try {
                walker.go(this.visitor);
            } catch (Exception e) {
                e.printStackTrace();
                throw new WriteZNodeDataErrorException("Fail to run topicTask.", e);
            }
        }
        System.out.println("Info: task.TopicTask end. " + Thread.currentThread().getName());
        return Thread.currentThread().getName();
    }
}
