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
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

public final class WorkerApplication {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerApplication.class);

  private final InetSocketAddress host;
  private final String zkConnectString;
  private final int id;

  private ActorSystem system;
  private ZooKeeper zk;

  public WorkerApplication(int id, InetSocketAddress host, String zkConnectString) {
    this.id = id;
    this.host = host;
    this.zkConnectString = zkConnectString;
  }

  public static void main(String... args) throws UnknownHostException {
    if (args.length > 0) {
      WorkerApplication.runWithArgs(args);
    } else {
      WorkerApplication.runWithEnv();
    }
  }

  private static void runWithEnv() throws UnknownHostException {
    final int id = Integer.valueOf(System.getenv("DS_ID"));
    final InetAddress address = InetAddress.getByName(System.getenv("DS_HOST"));
    final int port = Integer.parseInt(System.getenv("DS_PORT"));
    final InetSocketAddress socketAddress = new InetSocketAddress(address, port);

    final String connectingString = System.getenv("DS_ZK");
    new WorkerApplication(id, socketAddress, connectingString).run();
  }

  private static void runWithArgs(String... args) {
    final Options options = new Options();
    final Option idOpt = Option.builder("id").hasArg().argName("id").desc("worker id").required().build();
    final Option hostOpt = Option.builder("host").hasArg().argName("FQDN").desc("worker FQDN").required().build();
    final Option portOpt = Option.builder("port").hasArg().argName("port").desc("worker port").required().build();
    final Option zkOpt = Option.builder("zk").hasArg().argName("connectString").desc("ZK connect string").required().build();

    options.addOption(idOpt);
    options.addOption(hostOpt);
    options.addOption(portOpt);
    options.addOption(zkOpt);

    final CommandLineParser parser = new DefaultParser();

    try {
      final CommandLine cmd = parser.parse(options, args);
      final int id = Integer.valueOf(cmd.getOptionValue("id"));
      final InetAddress address = InetAddress.getByName(cmd.getOptionValue("host"));
      final int port = Integer.parseInt(cmd.getOptionValue("port"));
      final InetSocketAddress socketAddress = new InetSocketAddress(address, port);

      final String connectingString = cmd.getOptionValue("zk");
      new WorkerApplication(id, socketAddress, connectingString).run();
    } catch (ParseException e) {
      WorkerApplication.LOG.error("Parsing failed", e);
      final HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("dataStreams", options);
    } catch (UnknownHostException e) {
      WorkerApplication.LOG.error("Resolving failed", e);
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

      final ActorRef concierge = this.system.actorOf(NodeConcierge.props(this.id, this.zk), String.valueOf(this.id));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    try {
      Await.ready(this.system.terminate(), Duration.Inf());
      this.zk.close();
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
