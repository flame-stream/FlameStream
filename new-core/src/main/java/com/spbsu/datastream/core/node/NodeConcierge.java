package com.spbsu.datastream.core.node;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.configuration.HashRangeDeserializer;
import com.spbsu.datastream.core.front.FrontActor;
import com.spbsu.datastream.core.range.RangeConcierge;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class NodeConcierge extends LoggingActor {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    final SimpleModule module = new SimpleModule();
    module.addKeyDeserializer(HashRange.class, new HashRangeDeserializer());
    NodeConcierge.MAPPER.registerModule(module);
  }

  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());
  private final ZooKeeper zooKeeper;
  private final InetSocketAddress myAddress;
  private final int id;

  private final List<ActorRef> deployees = new ArrayList<>();

  private NodeConcierge(final int id, final InetSocketAddress myAddress, final ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
    this.myAddress = myAddress;
    this.id = id;
  }

  public static Props props(final int id, final InetSocketAddress address, final ZooKeeper zooKeeper) {
    return Props.create(NodeConcierge.class, id, address, zooKeeper);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();

    final Map<Integer, InetSocketAddress> dns = this.fetchDNS();
    this.LOG.info("DNS fetched: {}", dns);

    final Map<HashRange, Integer> ranges = this.fetchRangeMappings();
    this.LOG.info("Ranges fetched: {}", ranges);

    final Map<HashRange, InetSocketAddress> resolved = ranges.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> dns.get(e.getValue())));
    final ActorRef rootRouter = this.context().actorOf(RootRouter.props(resolved), "rootRouter");

    final Set<HashRange> myRanges = this.myRanges(ranges);
    this.deployees.addAll(myRanges.stream().map(r -> this.conciergeForRange(r, rootRouter)).collect(Collectors.toList()));

    final Set<Integer> fronts = this.fetchFronts();
    this.LOG.info("Fronts fetched: {}", fronts);

    if (fronts.contains(this.id)) {
      this.deployees.add(this.context().actorOf(FrontActor.props(rootRouter, this.id), "front"));
    }

    this.context().actorOf(TickCurator.props(this.zooKeeper, this.self()), "curator");
  }

  private Map<Integer, InetSocketAddress> fetchDNS() throws IOException, KeeperException, InterruptedException {
    final String path = "/dns";
    final byte[] data = this.zooKeeper.getData(path, this.selfWatcher(), new Stat());
    return NodeConcierge.MAPPER.readValue(data, new TypeReference<Map<Integer, InetSocketAddress>>() {
    });
  }

  private Map<HashRange, Integer> fetchRangeMappings() throws KeeperException, InterruptedException, IOException {
    final String path = "/ranges";
    final byte[] data = this.zooKeeper.getData(path, this.selfWatcher(), new Stat());
    return NodeConcierge.MAPPER.readValue(data, new TypeReference<Map<HashRange, Integer>>() {
    });
  }

  private Set<HashRange> myRanges(final Map<HashRange, Integer> mappings) {
    return mappings.entrySet().stream().filter(e -> e.getValue().equals(this.id))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
  }

  private ActorRef conciergeForRange(final HashRange range, final ActorRef remoteRouter) {
    return this.context().actorOf(RangeConcierge.props(range, remoteRouter), range.toString());
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

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof TickInfo) {
      this.deployees.forEach(d -> d.tell(message, ActorRef.noSender()));
    } else {
      this.unhandled(message);
    }
  }
}
