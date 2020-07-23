package demo.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class NameNodeRpcServer implements DFSProtocol {
    public static void main(String[] args) throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(9999)
                .setProtocol(DFSProtocol.class)
                .setInstance(new NameNodeRpcServer());

        RPC.Server build = builder.build();
        build.start();
    }

    @Override
    public void mkdir(String path) {
        System.out.println("Create path: " + path);
    }
}
