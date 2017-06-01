/*
 * Copyright 2015, Google Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *
 *    * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.grpc.examples.helloworld;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.Date;
import java.util.logging.Logger;
import java.text.SimpleDateFormat;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

class ZookeeperConnection {
  private static final Logger logger = Logger.getLogger("ZooKeeper");
  private ZooKeeper zoo;
  public void ZookeeperConnection() {
  }

  /**
   * Connects to a zookeeper ensemble in zkUriStr.
   * serverIp and portStr are the IP/Port of this server.
   */
  public boolean connect(String zkUriStr, String serverIp, String portStr)
                         throws IOException,InterruptedException {
    final CountDownLatch connectedSignal = new CountDownLatch(1);
    String zkhostport;
    try {
      URI zkUri = new URI(zkUriStr);
      zkhostport = zkUri.getHost().toString() + ":" + Integer.toString(zkUri.getPort());
    } catch (Exception e) {
      logger.severe("Could not parse zk URI " + zkUriStr);
      return false;
    }

    zoo = new ZooKeeper(zkhostport, 5000, new Watcher() {
      public void process(WatchedEvent we) {
        if (we.getState() == KeeperState.SyncConnected) {
          connectedSignal.countDown();
        }
      }
    });
    /* Wait for zookeeper connection */
    connectedSignal.await();

    String path = "/grpc_hello_world_service";
    Stat stat;
    String currTime = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
    try {
      stat = zoo.exists(path, true);
      if (stat == null) {
        zoo.create(path, currTime.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (Exception e) {
      logger.severe("Failed to create path");
      return false;
    }

    String server_addr = path + "/" + serverIp + ":" + portStr;
    try {
      stat = zoo.exists(server_addr, true);
      if (stat == null) {
        try {
          zoo.create(server_addr, currTime.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
          logger.severe("Failed to create server_data");
          return false;
        }
      } else {
        try {
          zoo.setData(server_addr, currTime.getBytes(), stat.getVersion());
        } catch (Exception e) {
          logger.severe("Failed to update server_data");
          return false;
        }
      }
    } catch (Exception e) {
      logger.severe("Failed to add server_data");
      return false;
    }
    return true;
  }

  // Method to disconnect from zookeeper server
  public void close() throws InterruptedException {
    zoo.close();
  }
}

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class HelloWorldServer {
  static String portStr;
  private static final Logger logger = Logger.getLogger(HelloWorldServer.class.getName());

  private Server server;

  private void start(String port) throws IOException {
    /* The port on which the server should run */
    server = ServerBuilder.forPort(Integer.parseInt(port))
        .addService(new GreeterImpl())
        .build()
        .start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        HelloWorldServer.this.stop();
        System.err.println("*** server shut down");
      }
    });
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }


  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    /* Argument parsing */
    if (args.length != 2) {
      System.out.println("Usage: helloworld_server PORT zk://ADDR:PORT");
      return;
    }

    String zk_addr;

    try {
      portStr = new String(args[0]);
      zk_addr = new String(args[1]);
    } catch (Exception e) {
      System.out.println("Usage: helloworld_server PORT zk://ADDR:PORT");
      return;
    }

    ZookeeperConnection zk_conn = new ZookeeperConnection();
    if (!zk_conn.connect(zk_addr, "localhost", portStr)) {
      return;
    }

    final HelloWorldServer server = new HelloWorldServer();
    server.start(portStr);
    server.blockUntilShutdown();
  }

  static class GreeterImpl extends GreeterGrpc.GreeterImplBase {

    @Override
    public void sayHello(HelloRequest req,
                         StreamObserver<HelloReply>responseObserver) {
      HelloReply reply = HelloReply.newBuilder().setMessage(
                         "Hello " + req.getName() + " from " + portStr).build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }
}
