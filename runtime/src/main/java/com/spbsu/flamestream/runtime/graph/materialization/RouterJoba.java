package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.config.HashRange;
import com.spbsu.flamestream.runtime.graph.GraphManager;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

import java.util.stream.Stream;

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

  public RouterJoba(IntRangeMap<ActorRef> router,
                    HashRange localRange,
                    HashFunction hashFunction,
                    GraphManager.Destination destination,
                    ActorRef acker,
                    ActorContext context) {
    this.hashFunction = hashFunction;
    this.destination = destination;
    this.acker = acker;
    this.context = context;
    this.router = router;
    this.localRange = localRange;
  }

  @Override
  public boolean isAsync() {
    return false;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    final int hash = hashFunction.applyAsInt(dataItem);
    if (localJoba != null && hash >= localRange.from() && hash < localRange.to()) {
      localJoba.accept(dataItem, fromAsync);
    } else {
      router.get(hash).tell(new AddressedItem(dataItem, destination), context.self());
      acker.tell(new Ack(dataItem.meta().globalTime(), dataItem.xor()), context.self());
    }
  }

  private final Tracing.Tracer tracer = Tracing.TRACING.forEvent("flatmap-send");
  public void accept(Stream<DataItem> dataItemStream, boolean fromAsync) {
    final long[] xor = {0};
    final GlobalTime[] globalTime = {null};
    dataItemStream.forEach(dataItem -> {
      final int hash = hashFunction.applyAsInt(dataItem);
      if (localJoba != null && hash >= localRange.from() && hash < localRange.to()) {
        localJoba.accept(dataItem, fromAsync);
      } else {
        tracer.log(dataItem.xor());
        router.get(hash).tell(new AddressedItem(dataItem, destination), context.self());
        globalTime[0] = dataItem.meta().globalTime();
        xor[0] ^= dataItem.xor();
      }
    });
    if (globalTime[0] != null) {
      acker.tell(new Ack(globalTime[0], xor[0]), context.self());
    }
  }

  public void setLocalJoba(Joba localJoba) {
    this.localJoba = localJoba;
  }
}
