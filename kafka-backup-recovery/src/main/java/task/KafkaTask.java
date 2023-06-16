package task;

import jobs.FSVisitor;
import utils.CommonUtils;

import java.util.concurrent.Callable;

public class KafkaTask implements Callable<String> {
    private final FSVisitor visitor;
    private final String inputDir;

    public KafkaTask(FSVisitor visitor, String inputDir) {
        this.visitor = visitor;
        this.inputDir = inputDir;
    }

    public String call() {
        return CommonUtils.writeDataToZookeeper("Kafka", this.inputDir, this.visitor);
    }
}
