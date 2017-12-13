package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.runtime.graph.materialization.Materializer;
import com.spbsu.flamestream.runtime.graph.materialization.Router;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;

/**
 * User: Artem
 * Date: 13.12.2017
 */
public class RouterJoba implements Joba {
  private final IntRangeMap<Router> routers;
  private final HashFunction hashFunction;
  private final Materializer.Destination destination;

  RouterJoba(IntRangeMap<Router> routers, HashFunction hashFunction, Materializer.Destination destination) {
    this.routers = routers;
    this.hashFunction = hashFunction;
    this.destination = destination;
  }

  @Override
  public boolean isAsync() {
    return true;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    routers.get(hashFunction.applyAsInt(dataItem)).route(dataItem, destination);
  }
}
