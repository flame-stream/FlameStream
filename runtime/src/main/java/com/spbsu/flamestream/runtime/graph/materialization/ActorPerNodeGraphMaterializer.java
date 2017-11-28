package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.graph.FlameRouter;
import com.spbsu.flamestream.runtime.graph.materialization.api.AddressedItem;
import com.spbsu.flamestream.runtime.graph.materialization.jobas.ActorJoba;
import com.spbsu.flamestream.runtime.graph.materialization.jobas.Joba;
import org.jooq.lambda.Unchecked;

import java.util.HashMap;
import java.util.Map;

public class ActorPerNodeGraphMaterializer implements GraphMaterializer {
  private final FlameRouter flameRouter;
  private final ActorRef acker;
  private final ActorRef barrier;

  public ActorPerNodeGraphMaterializer(FlameRouter flameRouter, ActorRef acker, ActorRef barrier) {
    this.flameRouter = flameRouter;
    this.acker = acker;
    this.barrier = barrier;
  }

  @Override
  public GraphMaterialization materialize(Graph graph) {
    final Map<String, ActorJoba> materializations = new HashMap<>();
    return new GraphMaterialization() {
      @Override
      public void accept(DataItem<?> dataItem) {

      }

      @Override
      public void inject(AddressedItem addressedItem) {
        materializations.get(addressedItem.vertexId()).accept(addressedItem.item());
      }

      @Override
      public void onMinTimeUpdate(GlobalTime globalTime) {
        materializations.values().forEach(materialization -> materialization.onMinTime(globalTime));
      }

      @Override
      public void onCommit() {
        materializations.values().forEach(Joba::onCommit);
      }

      @Override
      public void close() throws Exception {
        materializations.values().forEach(Unchecked.consumer(ActorJoba::close));
      }
    };
  }
}
