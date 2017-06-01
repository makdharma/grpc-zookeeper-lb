# grpc-zookeeper-lb
This is a toy example of using Zookeeper for load balancing with gRPC. To run:
1. Compile HelloWorldClient and HelloWorldServer.

```git clone https://github.com/makdharma/grpc-zookeeper-lb```

```cd grpc-zookeeper-lb/examples; ./gradelw installDist```

2. Download zookeeper stock docker image and start zookeeper.

```docker pull zookeeper```

```docker run -p 2181:2181 --restart always -d zookeeper```

3. Start couple of servers

```./build/install/examples/bin/hello-world-server 50000 zk://localhost:2181```

```./build/install/examples/bin/hello-world-server 50001 zk://localhost:2181```

4. Run hello-world-client. It should alternate between two servers.

```./build/install/examples/bin/hello-world-client zk://localhost:2181```

