package jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DumpJob implements Job, Watcher {
    private final String zkServer;
    private final String outputDir;
    private final String znode;

    public DumpJob(String zkServer, String outputDir, String znode) {
        this.zkServer = zkServer;
        this.outputDir = outputDir;
        this.znode = znode;
    }

    @Override
    public void go() {
        System.out.println("Info: Running dump job.");
        System.out.println(String.format("Info Zookeeper server: %s", this.zkServer));
        System.out.println(String.format("Info Reading from zookeeper path: %s", this.znode));
        System.out.println(String.format("Info Dumping to local directory: %s", this.outputDir));

        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper(this.zkServer + this.znode, 60_000, this);
            go(zk);
        } catch (IOException ioe) {
            System.err.println("Error: Error connecting to " + this.zkServer + " when execute dumpJob.");
        } catch (Exception e) {
            System.err.println(e.getMessage());
        } finally {
            try {
                if (zk != null) {
                    zk.close();
                }
            } catch (Exception e) {
                System.err.println("Warn: Close zookeeper error.");
            }
        }
    }

    private void go(ZooKeeper zk) throws Exception {
        while (!zk.getState().isConnected()) {
            System.out.println("Warn: Re-Connecting to " + this.zkServer + " with chroot " + this.znode);
            Thread.sleep(1_000L);
        }

        dumpChild(zk, this.outputDir + this.znode, "", "");
        System.out.println("Info: Dump job is complete.");
    }

    private void dumpChild(ZooKeeper zk, String outputDir, String znodeParent, String znode) throws Exception {
        Stat stat = new Stat();
        int retryNum = 3;
        boolean retryFlag = true;
        String znodePath = znodeParent + znode;
        String currznode = (znodePath.length() == 0) ? "/" : znodePath;

        while (--retryNum > 0 && retryFlag) {
            try {
                List<String> children;
                try {
                    children = zk.getChildren(currznode, false, stat);
                } catch (KeeperException.NoNodeException nne) {
                    System.out.println("Error: Node not exist, znodePath is : " + znodePath);
                    children = null;
                }

                if (stat.getEphemeralOwner() == 0L) {
                    File f = new File(outputDir);
                    boolean mkdirsResult = f.mkdirs();
                    System.out.println("Mkdirs " + outputDir + " result: " + mkdirsResult);

                    writeZnode(zk, outputDir + "/_znode", outputDir + "/_acl", currznode);
                    if (children != null) {
                        for (String c : children) {
                            dumpChild(zk, outputDir + "/" + c, znodePath + "/", c);
                        }
                    }
                }
                retryFlag = false;
            } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException se) {
                Thread.sleep(1_000L);
            }
        }
    }

    private void writeZnode(ZooKeeper zk, String outFile, String aclFile, String znode) throws Exception {
        Stat stat = new Stat();
        byte[] data;
        List<ACL> acls;
        boolean retryFlag = true;
        int retryNum = 3;
        ObjectMapper mapper = new ObjectMapper();
        List<String> aclToStringList = new ArrayList<>();

        while (retryNum-- > 0 && retryFlag) {
            try (FileOutputStream aclFileOutputStream = new FileOutputStream(aclFile);
                 FileOutputStream dataFileOutoutStream = new FileOutputStream(outFile)) {
                data = zk.getData(znode, false, stat);
                acls = zk.getACL(znode, stat);

                if (data != null && data.length > 0) {
                    String str = new String(data);
                    if (!str.equals("null")) {
                        dataFileOutoutStream.write(data);
                        dataFileOutoutStream.flush();
                    }
                }

                if (acls != null && !acls.isEmpty()) {
                    for (ACL acl : acls) {
                        aclToStringList.add(mapper.writeValueAsString(acl));
                    }
                }

                for (String aclToString : aclToStringList) {
                    if (stat.getEphemeralOwner() == 0L && null != aclToString && !aclToString.equals("null")) {
                        aclFileOutputStream.write((aclToString + "\n").getBytes());
                        aclFileOutputStream.flush();
                    }
                }
                retryFlag = false;
            } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
                Thread.sleep(1_000L);
            }
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {

    }
}
