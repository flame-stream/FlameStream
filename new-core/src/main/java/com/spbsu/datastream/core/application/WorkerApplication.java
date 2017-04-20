package com.spbsu.datastream.core.application;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.node.LifecycleWatcher;
import com.spbsu.datastream.core.node.NodeConcierge;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.cli.*;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class WorkerApplication {
  public static void main(final String... args) throws IOException {
    final Options options = new Options();
    final Option hostOpt = Option.builder("host").hasArg().argName("FQDN").desc("worker FQDN").required().build();
    final Option portOpt = Option.builder("port").hasArg().argName("port").desc("worker port").required().build();
    final Option zkOpt = Option.builder("zk").hasArg().argName("connectString").desc("ZK connect string").required().build();

    options.addOption(hostOpt);
    options.addOption(portOpt);
    options.addOption(zkOpt);

    final CommandLineParser parser = new DefaultParser();

    try {
      final CommandLine cmd = parser.parse(options, args);
      final InetAddress address = InetAddress.getByName(cmd.getOptionValue("host"));
      final int port = Integer.parseInt(cmd.getOptionValue("port"));
      final InetSocketAddress socketAddress = new InetSocketAddress(address, port);

      final String connectingString = cmd.getOptionValue("zk");
      new WorkerApplication().run(socketAddress, connectingString);
    } catch (final ParseException e) {
      System.err.println("Parsing failed.  Reason: " + e.getMessage());
      final HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("dataStreams", options);
      System.exit(1);
    }
  }

  public void run(final InetSocketAddress localHost, final String zkConnectString) throws IOException {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + localHost.getPort())
            .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + localHost.getHostString()))
            .withFallback(ConfigFactory.load("remote"));
    final ActorSystem system = ActorSystem.create("worker", config);
    final ActorRef watcher = system.actorOf(LifecycleWatcher.props(), "watcher");

    final ZooKeeper zooKeeper = new ZooKeeper(zkConnectString, 5000,
            event -> watcher.tell(event, null));

    final ActorRef concierge = system.actorOf(NodeConcierge.props(localHost, zooKeeper), "root");
  }
}
