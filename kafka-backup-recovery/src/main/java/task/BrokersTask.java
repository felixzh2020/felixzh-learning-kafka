package task;

import jobs.FSVisitor;
import utils.CommonUtils;

import java.util.concurrent.Callable;

public class BrokersTask implements Callable<String> {
    private final FSVisitor visitor;
    private final String inputDir;

    public BrokersTask(FSVisitor visitor, String inputDir) {
        this.visitor = visitor;
        this.inputDir = inputDir;
    }

    public String call() {
        return CommonUtils.writeDataToZookeeper("Brokers", this.inputDir, this.visitor);
    }
}
