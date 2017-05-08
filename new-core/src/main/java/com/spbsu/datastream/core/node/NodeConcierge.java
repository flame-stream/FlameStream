package com.spbsu.datastream.core.node;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.front.FrontActor;
import com.spbsu.datastream.core.tick.TickConcierge;
import com.spbsu.datastream.core.tick.TickInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

public final class NodeConcierge extends LoggingActor {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());
  private final ZooKeeper zooKeeper;
  private final int id;

  private ActorRef dnsRouter;
  private ActorRef tickRouter;

  private ActorRef front;

  private NodeConcierge(final int id, final ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
    this.id = id;
  }

  public static Props props(final int id, final ZooKeeper zooKeeper) {
    return Props.create(NodeConcierge.class, id, zooKeeper);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();

    final Map<Integer, InetSocketAddress> dns = this.fetchDNS();
    this.LOG.info("DNS fetched: {}", dns);

    this.tickRouter = this.context().actorOf(TickRouter.props());
    this.dnsRouter = this.context().actorOf(DNSRouter.props(dns, this.tickRouter, this.id), "dns");

    final Set<Integer> fronts = this.fetchFronts();
    this.LOG.info("Fronts fetched: {}", fronts);

    if (fronts.contains(this.id)) {
      this.front = this.context().actorOf(FrontActor.props(this.dnsRouter, this.id), "front");
    }

    this.context().actorOf(TickCurator.props(this.zooKeeper, this.self()), "curator");
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof TickInfo) {
      final TickInfo tickInfo = (TickInfo) message;
      final ActorRef tickConcierge = this.context().actorOf(TickConcierge.props(tickInfo, this.id, this.dnsRouter), String.valueOf(tickInfo.startTs()));
      this.tickRouter.tell(new TickRouter.RegisterTick(tickInfo.startTs(), tickConcierge), ActorRef.noSender());

      if (this.front != null) {
        this.front.tell(message, ActorRef.noSender());
      }
    } else {
      this.unhandled(message);
    }
  }

  private Map<Integer, InetSocketAddress> fetchDNS() throws IOException, KeeperException, InterruptedException {
    final String path = "/dns";
    final byte[] data = this.zooKeeper.getData(path, this.selfWatcher(), new Stat());
    return NodeConcierge.MAPPER.readValue(data, new TypeReference<Map<Integer, InetSocketAddress>>() {
    });
  }

  private Set<Integer> fetchFronts() throws KeeperException, InterruptedException, IOException {
    final String path = "/fronts";
    final byte[] data = this.zooKeeper.getData(path, this.selfWatcher(), new Stat());
    return NodeConcierge.MAPPER.readValue(data, new TypeReference<Set<Integer>>() {
    });
  }

  private Watcher selfWatcher() {
    return event -> this.self().tell(event, this.self());
  }
}
