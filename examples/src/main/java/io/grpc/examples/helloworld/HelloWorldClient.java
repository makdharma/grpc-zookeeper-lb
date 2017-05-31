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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import javax.annotation.Nullable;

import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.EquivalentAddressGroup;
//
// import zookeeper classes
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;


class ZkNameResolver extends NameResolver implements Watcher {
  /* Hard-coded path to the ZkNode that knows about servers */
  public static final String path = "/grpc_hello_world_service";
  /* 2 seconds to detect that server disconnected */
  public static final int kTimeoutMs = 2000;

  private static URI zk_uri;
  private static ZooKeeper zoo;
  private static Listener listener;
  private static final Logger logger = Logger.getLogger("ZK");

  /*
   * The callback from Zookeeper when servers are added/removed.
   */
  public void process(WatchedEvent we) {
    if (we.getType() == Event.EventType.None) {
      logger.info("Connection expired");
    } else {
      try {
        List<String> servers  = zoo.getChildren(path, false);
        AddServersToListener(servers);
        zoo.getChildren(path, this);
      } catch(Exception ex) {
        logger.info(ex.getMessage());
      }
    }
  }

  public void AddServersToListener(List<String> servers) {
    List<EquivalentAddressGroup> addrs = new ArrayList<EquivalentAddressGroup>();
    logger.info("Updating server list");
    for (String child : servers) {
      try {
        logger.info("Online: " + child);
        URI uri = new URI("dummy://" + child);
        /* Convert "host:port" into host and port */
        String host = uri.getHost();
        int port = uri.getPort();
        List<SocketAddress> sockaddrs_list= new ArrayList<SocketAddress>();
        sockaddrs_list.add(new InetSocketAddress(host, port));
        EquivalentAddressGroup eag =  new EquivalentAddressGroup(sockaddrs_list);
        addrs.add(eag);
      } catch(Exception ex) {
        logger.info("Unparsable server address: "+child);
        logger.info(ex.getMessage());
      }
    }
    if (addrs.size() > 0) {
      listener.onAddresses(addrs, Attributes.EMPTY);
    } else {
      logger.info("No servers online. Keep looking");
    }
  }


  public ZkNameResolver (URI zk_uri) {
    this.zk_uri = zk_uri;
  }

  @Override
  public String getServiceAuthority() {
    return zk_uri.getAuthority();
  }

  @Override
  public void start(Listener listener) {
    this.listener = listener;
    final CountDownLatch connectedSignal = new CountDownLatch(1);
    try {
      String zkaddr = zk_uri.getHost().toString() + ":" + Integer.toString(zk_uri.getPort());
      logger.info("Connecting to Zookeeper Address " + zkaddr);

      this.zoo = new ZooKeeper(zkaddr, kTimeoutMs, new Watcher() {
        public void process(WatchedEvent we) {
          if (we.getState() == KeeperState.SyncConnected) {
            connectedSignal.countDown();
          }
        }
      });
      connectedSignal.await();
      logger.info("Connected!");
    } catch (Exception e) {
      logger.info("Failed to connect");
      return;
    }


    try {
      Stat stat = zoo.exists(path, true);
      if (stat == null) {
        logger.info("Path does not exist.");
      } else {
        logger.info("Path exists");
      }
    } catch (Exception e) {
      logger.info("Failed to get stat");
      return;
    }

    try {
      final CountDownLatch connectedSignal1 = new CountDownLatch(1);
      List<String> servers = zoo.getChildren(path, this);
      AddServersToListener(servers);
    } catch(Exception e) {
      logger.info(e.getMessage());
    }
  }

  @Override
  public void shutdown() {
  }
}

class ZkNameResolverProvider extends NameResolverProvider {
  @Nullable
  @Override
  public NameResolver newNameResolver(URI targetUri, Attributes params) {
    return new ZkNameResolver(targetUri);
  }

  @Override
  protected int priority() {
    return 5;
  }

  @Override
  protected boolean isAvailable() {
    return true;
  }

  @Override
  public String getDefaultScheme() {
    return "zk";
  }
}

/**
 * A simple client that requests a greeting from the {@link HelloWorldServer},
 * and round robin load-balances among all available servers.
 */
public class HelloWorldClient {
  private static final Logger logger = Logger.getLogger("Client");

  private final ManagedChannel channel;
  private final GreeterGrpc.GreeterBlockingStub blockingStub;

  /** Construct client connecting to HelloWorld server using Zookeeper name resolver
   * and Round Robin load balancer. */
  public HelloWorldClient(String zk_addr) {
    this(ManagedChannelBuilder.forTarget(zk_addr)
        .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
        .nameResolverFactory(new ZkNameResolverProvider())
        .usePlaintext(true));
  }

  /** Construct client for accessing the server using the existing channel. */
  HelloWorldClient(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.build();
    blockingStub = GreeterGrpc.newBlockingStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  /** Say hello to server. */
  public void greet() {
    HelloRequest request = HelloRequest.newBuilder().setName("world").build();
    HelloReply response;
    try {
      response = blockingStub.sayHello(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    logger.info("Greeting: " + response.getMessage());
  }

  /**
   * Greet client. If provided, the first argument of {@code args} is the address
   * of the Zookeeper ensemble. The client keeps making simple RPCs until interrupted
   * with a Ctrl-C.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("Usage: helloworld_client zk://ADDR:PORT");
      return;
    }
    HelloWorldClient client = new HelloWorldClient(args[0]);
    try {
      while(true) {
        client.greet();
        Thread.sleep(1000);
      }
    } finally {
      client.shutdown();
    }
  }
}
