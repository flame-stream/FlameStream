package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.config.HashRange;
import com.spbsu.flamestream.runtime.graph.GraphManager;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;
import com.spbsu.flamestream.runtime.utils.collections.ListIntRangeMap;
import org.apache.commons.lang.math.IntRange;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Artem
 * Date: 13.12.2017
 */
public class RouterJoba implements Joba {
  private final IntRangeMap<ActorRef> router;
  private final HashFunction hashFunction;
  private final GraphManager.Destination destination;
  private final ActorRef acker;
  private final ActorContext context;
  private final HashRange localRange;

  private Joba localJoba;

  public RouterJoba(String nodeId,
                    ComputationProps layout,
                    Map<String, ActorRef> managerRefs,
                    HashFunction hashFunction,
                    GraphManager.Destination destination,
                    ActorRef acker,
                    ActorContext context) {
    this.hashFunction = hashFunction;
    this.destination = destination;
    this.acker = acker;
    this.context = context;

    final Map<IntRange, ActorRef> routerMap = new HashMap<>();
    layout.ranges().forEach((key, value) -> routerMap.put(value.asRange(), managerRefs.get(key)));
    router = new ListIntRangeMap<>(routerMap);
    localRange = layout.ranges().get(nodeId);
  }

  @Override
  public boolean isAsync() {
    return false;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    final int hash = hashFunction.applyAsInt(dataItem);
    if (hash >= localRange.from() && hash < localRange.to()) {
      localJoba.accept(dataItem, fromAsync);
    } else {
      router.get(hash).tell(new AddressedItem(dataItem, destination), context.self());
      acker.tell(new Ack(dataItem.meta().globalTime(), dataItem.xor()), context.self());
    }
  }

  public void setLocalJoba(Joba localJoba) {
    this.localJoba = localJoba;
  }
}
