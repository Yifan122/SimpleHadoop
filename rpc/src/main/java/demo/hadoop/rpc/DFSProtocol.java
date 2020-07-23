package demo.hadoop.rpc;

public interface DFSProtocol {
    long versionID = 123L;

    // hadoop fs -mkdir
    void mkdir(String path);
}
