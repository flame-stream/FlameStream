package com.spbsu.datastream.core.application;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.node.LifecycleWatcher;
import com.spbsu.datastream.core.node.NodeConcierge;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public final class WorkerApplication {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerApplication.class);

  private final InetSocketAddress host;
  private final String zkConnectString;

  private ActorSystem system;
  private ZooKeeper zk;

  public WorkerApplication(final InetSocketAddress host, final String zkConnectString) {
    this.host = host;
    this.zkConnectString = zkConnectString;
  }

  public static void main(final String... args) throws UnknownHostException {
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
      new WorkerApplication(socketAddress, connectingString).run();
    } catch (final ParseException e) {
      WorkerApplication.LOG.error("Parsing failed", e);
      final HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("dataStreams", options);
    }
  }

  public void run() {
    try {
      final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + this.host.getPort())
              .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + this.host.getHostString()))
              .withFallback(ConfigFactory.load("remote"));
      this.system = ActorSystem.create("worker", config);
      final ActorRef watcher = this.system.actorOf(LifecycleWatcher.props(), "watcher");

      this.zk = new ZooKeeper(this.zkConnectString, 5000,
              event -> watcher.tell(event, null));

      final ActorRef concierge = this.system.actorOf(NodeConcierge.props(this.host, this.zk), "root");
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    // TODO: 5/2/17 Graceful stop
    this.system.shutdown();
    try {
      this.zk.close();
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
