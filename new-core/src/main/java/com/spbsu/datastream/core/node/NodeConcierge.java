package com.spbsu.datastream.core.node;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.routing.RemoteRouter;
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

public class NodeConcierge extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());

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
    LOG.info("Starting... Address: {}", address);

    final RangeMappingsDto mappings = fetchMappings();
    LOG.info("Mappings fetched: {}", mappings);

    final ActorRef remoteRouter = remoteRouter(mappings);
    final Set<HashRange> myRanges = myRanges(mappings);

    myRanges.forEach(r -> forRange(r, remoteRouter));
  }

  @Override
  public void postStop() throws Exception {
    LOG.info("Stopped");
    super.postStop();
  }

  @Override
  public void preRestart(final Throwable reason, final Option<Object> message) throws Exception {
    LOG.error("Restarting, reason: {}, message: {}", reason, message);
    super.preRestart(reason, message);
  }

  private ActorRef forRange(final HashRange range, final ActorRef remoteRouter) {
    return context().actorOf(RangeConcierge.props(range, remoteRouter), range.toString());
  }

  private Set<HashRange> myRanges(final RangeMappingsDto mappings) {
    return mappings.rangeMappings().entrySet().stream().filter(e -> e.getValue().equals(address))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
  }

  private ActorRef remoteRouter(final RangeMappingsDto mappings) {
    final Map<HashRange, ActorSelection> remotes = mappings.rangeMappings().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> remoteDispatcher(e.getValue())));
    return context().actorOf(RemoteRouter.props(remotes));
  }

  private ActorSelection remoteDispatcher(final InetSocketAddress socketAddress) {
    final Address address = Address.apply("akka.tcp", "system",
            socketAddress.getAddress().getCanonicalHostName(),
            socketAddress.getPort());
    final ActorPath dispatcher = RootActorPath.apply(address, "/").$div("dispatcher");
    return context().system().actorSelection(dispatcher);
  }

  private RangeMappingsDto fetchMappings() throws KeeperException, InterruptedException, IOException {
    final String path = "/member/" + address.getHostString() + ":" + address.getPort();
    final byte[] data = zooKeeper.getData(path, selfWatcher(), new Stat());
    return mapper.readValue(data, RangeMappingsDto.class);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
  }

  private Watcher selfWatcher() {
    return event -> self().tell(event, self());
  }
}
