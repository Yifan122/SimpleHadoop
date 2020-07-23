package demo.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class DFSClient {
    public static void main(String[] args) throws IOException {
        DFSProtocol proxy = RPC.getProxy(DFSProtocol.class, 123L, new InetSocketAddress("localhost", 9999), new Configuration());

        proxy.mkdir("/system/usr/warehose");
    }
}
