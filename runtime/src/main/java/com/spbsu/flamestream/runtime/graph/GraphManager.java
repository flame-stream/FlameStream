package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.config.ComputationLayout;
import com.spbsu.flamestream.runtime.config.HashRange;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.graph.api.Commit;
import com.spbsu.flamestream.runtime.graph.materialization.ActorPerNodeMaterializer;
import com.spbsu.flamestream.runtime.graph.materialization.vertices.VertexJoba;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.jooq.lambda.Unchecked;

import java.util.Map;
import java.util.function.BiConsumer;

public class GraphManager extends LoggingActor {
  private final String nodeId;
  private final Graph graph;
  private final ActorRef acker;
  private final ComputationLayout layout;
  private final BiConsumer<DataItem<?>, ActorRef> barrier;

  private Map<String, VertexJoba> materialization = null;

  private GraphManager(String nodeId,
                       Graph graph,
                       ActorRef acker,
                       ComputationLayout layout,
                       BiConsumer<DataItem<?>, ActorRef> barrier) {
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
                            BiConsumer<DataItem<?>, ActorRef> barrier) {
    return Props.create(GraphManager.class, nodeId, graph, acker, layout, barrier);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Map.class, managers -> {
              log().info("Finishing constructor");
              //noinspection unchecked
              materialization = new ActorPerNodeMaterializer(
                      graph,
                      routerSink(managers),
                      dataItem -> barrier.accept(dataItem, self()),
                      context()
              ).materialize();

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
            .match(GlobalTime.class, gt -> acker.tell(new Heartbeat(gt, gt.front(), nodeId), self()))
            .build();
  }

  @Override
  public void postStop() {
    materialization.values().forEach(Unchecked.consumer(AutoCloseable::close));
  }

  private void accept(DataItem<?> dataItem) {
    //noinspection unchecked
    materialization.get(graph.source().id()).accept(dataItem);
    ack(dataItem);
  }

  private void inject(AddressedItem addressedItem) {
    //noinspection unchecked
    materialization.get(addressedItem.vertexId()).accept(addressedItem.item());
    ack(addressedItem.item());
  }

  private void onMinTimeUpdate(MinTimeUpdate minTimeUpdate) {
    materialization.values().forEach(materialization -> materialization.onMinTime(minTimeUpdate.minTime()));
  }

  private void onCommit() {
    materialization.values().forEach(VertexJoba::onCommit);
  }

  private BiConsumer<DataItem<?>, HashFunction<DataItem<?>>> routerSink(Map<String, ActorRef> managerRefs) {
    // TODO: 01.12.2017 we lost optimization (acking once flatmap results)
    return (dataItem, hashFunction) -> {
      final int hash = hashFunction.applyAsInt(dataItem);
      for (Map.Entry<String, HashRange> entry : layout.ranges().entrySet()) {
        if (entry.getValue().from() <= hash && hash < entry.getValue().to()) {
          managerRefs.get(entry.getKey()).tell(dataItem, self());
          ack(dataItem);
          return;
        }
      }
      throw new IllegalStateException("Hash ranges doesn't cover Integer space");
    };
  }

  private void ack(DataItem<?> dataItem) {
    acker.tell(new Ack(dataItem.meta().globalTime(), dataItem.xor()), self());
  }
}
