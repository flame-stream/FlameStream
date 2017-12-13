package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.Commit;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.config.ComputationLayout;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.graph.materialization.Materializer;
import com.spbsu.flamestream.runtime.graph.materialization.Router;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;
import com.spbsu.flamestream.runtime.utils.collections.ListIntRangeMap;
import org.apache.commons.lang.math.IntRange;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class GraphManager extends LoggingActor {
  private final String nodeId;
  private final Graph graph;
  private final ActorRef acker;
  private final ComputationLayout layout;
  private final BiConsumer<DataItem, ActorRef> barrier;

  private Materializer materializer = null;

  private GraphManager(String nodeId,
                       Graph graph,
                       ActorRef acker,
                       ComputationLayout layout,
                       BiConsumer<DataItem, ActorRef> barrier) {
    this.nodeId = nodeId;
    this.layout = layout;
    this.graph = graph;
    this.acker = acker;
    this.barrier = barrier;
  }

  public static Props props(String nodeId,
                            Graph graph,
                            ActorRef acker,
                            ComputationLayout layout,
                            BiConsumer<DataItem, ActorRef> barrier) {
    return Props.create(GraphManager.class, nodeId, graph, acker, layout, barrier);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Map.class, managers -> {
              log().info("Finishing constructor");
              //noinspection unchecked
              materializer = new Materializer(
                      graph,
                      routers(managers),
                      dataItem -> barrier.accept(dataItem, self()),
                      dataItem -> acker.tell(new Ack(dataItem.meta().globalTime(), dataItem.xor()), self()),
                      globalTime -> acker.tell(new Heartbeat(globalTime), self()),
                      context()
              );

              unstashAll();
              getContext().become(managing());
            })
            .matchAny(m -> stash())
            .build();
  }

  private Receive managing() {
    return ReceiveBuilder.create()
            .match(DataItem.class, this::accept)
            .match(AddressedItem.class, this::inject)
            .match(MinTimeUpdate.class, this::onMinTimeUpdate)
            .match(Commit.class, commit -> onCommit())
            .match(Heartbeat.class, gt -> acker.forward(gt, context()))
            .build();
  }

  private void accept(DataItem dataItem) {
    materializer.materialization().input(dataItem, sender());
  }

  private void inject(AddressedItem addressedItem) {
    materializer.materialization().inject(addressedItem.destination(), addressedItem.item());
  }

  private void onMinTimeUpdate(MinTimeUpdate minTimeUpdate) {
    materializer.materialization().minTime(minTimeUpdate.minTime());
  }

  private void onCommit() {
    materializer.materialization().commit();
  }

  private IntRangeMap<Router> routers(Map<String, ActorRef> managerRefs) {
    final Map<IntRange, Router> routerMap = new HashMap<>();
    layout.ranges().forEach((key, value) -> routerMap.put(
            value.asRange(),
            (dataItem, destination) -> managerRefs.get(key).tell(new AddressedItem(dataItem, destination), self())
    ));
    return new ListIntRangeMap<>(routerMap);
  }
}
