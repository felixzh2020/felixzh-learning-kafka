package jobs;

import org.apache.zookeeper.data.ACL;

import java.io.File;
import java.util.List;

public interface FSVisitor {
    void visit(File paramFile, byte[] paramArrayOfbyte, List<ACL> paramList, String parmString) throws Exception;
}
