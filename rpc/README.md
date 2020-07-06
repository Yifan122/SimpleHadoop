# RPC
An simple RPC framework for the SimpleHadoop

### How to use
1. Define an interface:

		public interface HelloService {
            String hello(String name);
        }

2. Implement the interface with annotation @RpcService:

		public class HelloServiceImpl implements HelloService {
            public HelloServiceImpl() {
            }
            @Override
            public String hello(String name) {
                return "Server: Hello " + name;
            }
        }

3. Config server (rpc.properties):

        # rpc server
        server.hostname=127.0.0.1
        server.port=18866
    

4. Start server:

   Start server with spring: com.yifan.simple.hadoop.bootstrap.NettyRpcServerBootstrap

   Start server without spring: com.yifan.simple.hadoop.bootstrap.NettyRpcClientBootstrap

 
		
