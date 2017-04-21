package com.spbsu.datastream.core.node;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.range.RangeConcierge;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import scala.Option;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class NodeConcierge extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());

  private final ZooKeeper zooKeeper;
  private final InetSocketAddress address;
  private final ObjectMapper mapper = new ObjectMapper();

  private NodeConcierge(final InetSocketAddress address, final ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
    this.address = address;
  }

  public static Props props(final InetSocketAddress address, final ZooKeeper zooKeeper) {
    return Props.create(NodeConcierge.class, address, zooKeeper);
  }

  @Override
  public void preStart() throws Exception {
    this.LOG.info("Starting... Address: {}", this.address);

    final RangeMappingsDto mappings = this.fetchMappings();
    this.LOG.info("Mappings fetched: {}", mappings);

    final ActorRef remoteRouter = this.remoteRouter(mappings);
    final Set<HashRange> myRanges = this.myRanges(mappings);

    myRanges.forEach(r -> this.conciergeForRange(r, remoteRouter));
    super.preStart();
  }

  @Override
  public void postStop() throws Exception {
    this.LOG.info("Stopped");
    super.postStop();
  }

  @Override
  public void preRestart(final Throwable reason, final Option<Object> message) throws Exception {
    this.LOG.error("Restarting, reason: {}, message: {}", reason, message);
    super.preRestart(reason, message);
  }

  private ActorRef conciergeForRange(final HashRange range, final ActorRef remoteRouter) {
    return this.context().actorOf(RangeConcierge.props(range, remoteRouter), range.toString());
  }

  private Set<HashRange> myRanges(final RangeMappingsDto mappings) {
    return mappings.rangeMappings().entrySet().stream().filter(e -> e.getValue().equals(this.address))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
  }

  private ActorRef remoteRouter(final RangeMappingsDto mappings) {
    return this.context().actorOf(RemoteRouter.props(this.remoteDispatchers(mappings)), "remoteRouter");
  }

  private Map<HashRange, ActorSelection> remoteDispatchers(final RangeMappingsDto mappings) {
    return mappings.rangeMappings().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> this.remoteDispatcher(e.getValue(), e.getKey())));
  }

  private ActorSelection remoteDispatcher(final InetSocketAddress socketAddress, final HashRange range) {
    final ActorPath dispatcher = MyPaths.rootRouter(socketAddress, range);
    return this.context().system().actorSelection(dispatcher);
  }

  private RangeMappingsDto fetchMappings() throws KeeperException, InterruptedException, IOException {
    final String path = "/mappings";
    final byte[] data = this.zooKeeper.getData(path, this.selfWatcher(), new Stat());
    return this.mapper.readValue(data, RangeMappingsDto.class);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    this.unhandled(message);
  }

  private Watcher selfWatcher() {
    return event -> this.self().tell(event, this.self());
  }
}
