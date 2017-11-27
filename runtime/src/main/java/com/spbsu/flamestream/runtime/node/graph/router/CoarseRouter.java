package com.spbsu.flamestream.runtime.node.graph.router;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.ComposedGraph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.runtime.node.graph.instance.api.AddressedItem;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;
import com.spbsu.flamestream.runtime.utils.collections.ListIntRangeMap;
import org.apache.commons.lang.math.IntRange;

import java.util.Map;
import java.util.function.ToIntFunction;

public class CoarseRouter implements FlameRouter {
  private final ComposedGraph<AtomicGraph> graph;
  private final IntRangeMap<ActorRef> hashRanges;

  public CoarseRouter(ComposedGraph<AtomicGraph> graph,
                      Map<IntRange, ActorRef> hashRanges) {
    this.graph = graph;
    this.hashRanges = new ListIntRangeMap<>(hashRanges);
  }

  @Override
  public void tell(DataItem<?> message, OutPort source, ActorRef sender) {
    final InPort destination = graph.downstreams().get(source);

    //noinspection rawtypes
    final ToIntFunction hashFunction = destination.hashFunction();

    //noinspection unchecked
    final int hash = hashFunction.applyAsInt(message.payload());
    final AddressedItem result = new AddressedItem(message, destination);

    final ActorRef ref = hashRanges.get(hash);
    ref.tell(result, sender);
  }
}
