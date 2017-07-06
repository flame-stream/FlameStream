package com.spbsu.datastream.core.application;

import akka.actor.ActorSystem;
import com.spbsu.datastream.core.node.LifecycleWatcher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

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

  public WorkerApplication(int id, InetSocketAddress host, String zkConnectString) {
    this.id = id;
    this.host = host;
    this.zkConnectString = zkConnectString;
  }

  public static void main(String... args) throws UnknownHostException {
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
    }
  }

  public void run() {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + this.host.getPort())
            .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + this.host.getHostString()))
            .withFallback(ConfigFactory.load("remote"));
    this.system = ActorSystem.create("worker", config);

    this.system.actorOf(LifecycleWatcher.props(this.zkConnectString, this.id), "watcher");
  }

  public void shutdown() {
    try {
      Await.ready(this.system.terminate(), Duration.Inf());
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
