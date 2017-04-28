package com.spbsu.datastream.core.node;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.configuration.RangeMappingsDto;
import com.spbsu.datastream.core.front.FrontActor;
import com.spbsu.datastream.core.range.RangeConcierge;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class NodeConcierge extends LoggingActor {
  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());

  private final ZooKeeper zooKeeper;
  private final InetSocketAddress myAddress;
  private final ObjectMapper mapper = new ObjectMapper();

  private NodeConcierge(final InetSocketAddress myAddress, final ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
    this.myAddress = myAddress;
  }

  public static Props props(final InetSocketAddress address, final ZooKeeper zooKeeper) {
    return Props.create(NodeConcierge.class, address, zooKeeper);
  }

  @Override
  public void preStart() throws Exception {
    this.LOG.info("Starting... Address: {}", this.myAddress);

    final Map<HashRange, InetSocketAddress> rangeMappings = this.fetchRangeMappings();
    this.LOG.info("Range mappings fetched: {}", rangeMappings);

    final ActorRef rootRouter = this.context().actorOf(RootRouter.props(rangeMappings), "rootRouter");

    final Set<HashRange> myRanges = this.myRanges(rangeMappings);
    myRanges.forEach(r -> this.conciergeForRange(r, rootRouter));

    final Map<InetSocketAddress, String> frontMappings = this.fetchFrontMappings();
    this.LOG.info("Front mappings fetched: {}", frontMappings);

    // TODO: 4/28/17 Front not starting
    Optional.ofNullable(frontMappings.get(this.myAddress))
            .ifPresent(id -> this.context().actorOf(FrontActor.props(rootRouter, id), "front"));

    super.preStart();
  }

  private ActorRef conciergeForRange(final HashRange range, final ActorRef remoteRouter) {
    return this.context().actorOf(RangeConcierge.props(range, remoteRouter), range.toString());
  }

  private Set<HashRange> myRanges(final Map<HashRange, InetSocketAddress> mappings) {
    return mappings.entrySet().stream().filter(e -> e.getValue().equals(this.myAddress))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
  }

  private Map<HashRange, InetSocketAddress> fetchRangeMappings() throws KeeperException, InterruptedException, IOException {
    final String path = "/ranges";
    final byte[] data = this.zooKeeper.getData(path, this.selfWatcher(), new Stat());
    return this.mapper.readValue(data, RangeMappingsDto.class).rangeMappings();
  }

  private Map<InetSocketAddress, String> fetchFrontMappings() throws KeeperException, InterruptedException, IOException {
    final String path = "/fronts";
    final byte[] data = this.zooKeeper.getData(path, this.selfWatcher(), new Stat());
    return this.mapper.readValue(data, new TypeReference<Map<String, InetSocketAddress>>() {
    });
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    this.unhandled(message);
  }

  private Watcher selfWatcher() {
    return event -> this.self().tell(event, this.self());
  }
}
