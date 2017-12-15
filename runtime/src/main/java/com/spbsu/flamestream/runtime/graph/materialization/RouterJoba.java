package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.runtime.config.ComputationLayout;
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
  private final ActorContext context;

  public RouterJoba(ComputationLayout layout,
                    Map<String, ActorRef> managerRefs,
                    HashFunction hashFunction,
                    GraphManager.Destination destination,
                    ActorContext context) {
    this.hashFunction = hashFunction;
    this.destination = destination;
    this.context = context;

    final Map<IntRange, ActorRef> routerMap = new HashMap<>();
    layout.ranges().forEach((key, value) -> routerMap.put(value.asRange(), managerRefs.get(key)));
    router = new ListIntRangeMap<>(routerMap);
  }

  @Override
  public boolean isAsync() {
    return true;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    router.get(hashFunction.applyAsInt(dataItem)).tell(new AddressedItem(dataItem, destination), context.self());
  }
}
