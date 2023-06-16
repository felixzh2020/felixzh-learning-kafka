package jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.data.ACL;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class FSWalker {
    private final String localRoot;
    private final String znodeRoot;
    private static final ObjectMapper mapper = new ObjectMapper();

    public FSWalker(String localRoot, String znodeRoot) {
        this.localRoot = localRoot;
        this.znodeRoot = znodeRoot;
    }

    public void go(FSVisitor visitor) throws Exception {
        File f = new File(this.localRoot);
        walk(visitor, f, this.znodeRoot);
    }

    private void walk(FSVisitor visitor, File file, String znode) throws Exception {
        List<ACL> acl = new ArrayList<>();
        byte[] data = new byte[0];

        if (znode.endsWith("_acl")) {
            znode = znode.substring(0, znode.length() - "acl".length() + 1);
            setACL(file, acl);
        } else {
            if (znode.endsWith("_znode")) {
                znode = znode.substring(0, znode.length() - "_znode".length());
            }
            if (znode.endsWith("/") && znode.length() > 1) {
                znode = znode.substring(0, znode.length() - 1);
            }
            try {
                data = FileUtils.readFileToByteArray(file);
            } catch (IOException ioException) {
            }
        }
        visitor.visit(file, data, acl, znode);
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            for (File child : children) {
                walk(visitor, child, (znode.equals("/") ? "" : znode) + "/" + child.getName());
            }
        }
    }

    private void setACL(File aclFile, List<ACL> aclList) {
        try {
            String sin = FileUtils.readFileToString(aclFile, Charset.defaultCharset());
            String[] out = sin.split("\n");
            for (String s : out) {
                if (s != null && !s.isEmpty()) {
                    ACL acl = (ACL) mapper.readValue(s, ACL.class);
                    aclList.add(acl);
                }
            }
        } catch (IOException ioe) {

        }
    }

    public static void createOne(FSVisitor visitor, File file, String znode) throws Exception {
        List<ACL> aclList = new ArrayList<>();
        byte[] data = new byte[0];
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            File znodeFile = null;
            File aclFile = null;
            for (File child : children) {
                if (child.getName().endsWith("_znode")) {
                    znodeFile = child;
                }
                if (child.getName().endsWith("_acl")) {
                    aclFile = child;
                }
            }
            if (null != znodeFile && znodeFile.exists()) {
                try {
                    data = FileUtils.readFileToByteArray(znodeFile);
                } catch (IOException e) {
                    System.out.println("Warn: Get data for znode " + znode + " failed.");
                }
            }
            if (null != aclFile && aclFile.exists()) {
                try {
                    aclList = new ArrayList<>();
                    String sin = FileUtils.readFileToString(aclFile);
                    String[] out = sin.split("\n");
                    for (String s : out) {
                        if (s != null && !s.isEmpty()) {
                            ACL acl = (ACL) mapper.readValue(s, ACL.class);
                            aclList.add(acl);
                        }
                    }
                } catch (IOException e) {
                    System.out.println("Warn: Get ACL for znode " + znode + " failed.");
                }
                visitor.visit(znodeFile, data, aclList, znode);
            }
        }
    }
}
