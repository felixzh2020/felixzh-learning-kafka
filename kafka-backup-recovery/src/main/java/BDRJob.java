import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import jobs.CleanJob;
import jobs.DumpJob;
import jobs.Job;
import jobs.RestoreJob;

public class BDRJob implements Job {
    private final CommandArgs commandArgs;
    private final JCommander jCommander;
    private Job job;

    public BDRJob(JCommander jCommander, CommandArgs commandArgs) {
        this.jCommander = jCommander;
        this.commandArgs = commandArgs;

        buildJob();
    }

    public static void main(String[] args) {
        CommandArgs commandArgs = new CommandArgs();
        JCommander jCommander = null;

        try {
            jCommander = JCommander.newBuilder().addObject(commandArgs).build();
            jCommander.parse(args);
        } catch (ParameterException e) {
            usage(jCommander);
        }

        BDRJob kafkaJob = new BDRJob(jCommander, commandArgs);
        kafkaJob.go();
    }

    public void buildJob() {
        if (this.commandArgs.server == null) {
            usage(this.jCommander);
        }

        String server = this.commandArgs.server;
        if (isNotNullOrEmpty(this.commandArgs.dumpZnode) && isNotNullOrEmpty(this.commandArgs.outputDir)) {
            this.job = new DumpJob(server, this.commandArgs.outputDir, this.commandArgs.dumpZnode);
        } else if (isNotNullOrEmpty(this.commandArgs.restoreZnode) && isNotNullOrEmpty(this.commandArgs.inputDir)) {
            this.job = new RestoreJob(server, this.commandArgs.restoreZnode, this.commandArgs.inputDir);
        } else if (isNotNullOrEmpty(this.commandArgs.cleanZnode)) {
            this.job = new CleanJob(server, this.commandArgs.cleanZnode);
        } else {
            usage(this.jCommander);
        }
    }

    public static class CommandArgs {
        @Parameter(names = {"-s", "--server"}, required = true, description = "zookeeper server (eg: local:2181)")
        private String server;

        @Parameter(names = {"-r", "--restore-znode"}, required = false, description = "the znode into which read data should be restored")
        private String restoreZnode;

        @Parameter(names = {"-i", "--input-dir"}, required = false, description = "the input directory from which znode information should be read")
        private String inputDir;

        @Parameter(names = {"-d", "--dump-znode"}, required = false, description = "the znode to dump (recursively)")
        private String dumpZnode;

        @Parameter(names = {"-o", "--putput-dir"}, required = false, description = "the output directory to which znode information should be written (must be a normal, empty directory)")
        private String outputDir;

        @Parameter(names = {"-c", "--clean-znode"}, required = false, description = "the znode to connect and clean kafka znode")
        private String cleanZnode;

        @Parameter(names = {"-v", "--verbose"}, required = false, description = "enable debug output")
        private boolean verbose;
    }

    @Override
    public void go() {
        this.job.go();
    }

    public static void usage(JCommander jCommander) {
        if (jCommander != null) {
            jCommander.usage();
        }
        System.exit(1);
    }

    private static boolean isNotNullOrEmpty(String str) {
        return (str != null && !str.isEmpty());
    }
}
