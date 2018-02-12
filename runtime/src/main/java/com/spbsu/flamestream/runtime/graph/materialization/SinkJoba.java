package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 12.12.2017
 */
public class SinkJoba extends Joba.Stub implements MinTimeHandler {
  private final InvalidatingBucket invalidatingBucket = new ArrayInvalidatingBucket();
  private final List<ActorRef> rears = new ArrayList<>();
  private int allItems;
  private int validItems;

  private final Tracing.Tracer tracer = Tracing.TRACING.forEvent("barrier-delay", 3000, 1);

  private final long[] docFlush = new long[3000];

  public SinkJoba(ActorRef acker, ActorContext context) {
    super(Stream.empty(), acker, context);
  }

  public void addRear(ActorRef rear) {
    rears.add(rear);
  }

  @Override
  public boolean isAsync() {
    return false;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    //docFlush[dataItem.payload(Object.class).hashCode()] = System.nanoTime();
    allItems++;
    //rears.forEach(rear -> rear.tell(dataItem, context.self()));
    invalidatingBucket.insert(dataItem);
    process(dataItem, Stream.of(dataItem), fromAsync);
  }

  @Override
  public void onMinTime(GlobalTime minTime) {
    final TIntSet outDocIds = new TIntHashSet();

    final int pos = invalidatingBucket.lowerBound(new Meta(minTime));
    invalidatingBucket.rangeStream(0, pos).forEach(di -> {
      validItems++;
      rears.forEach(rear -> {
        outDocIds.add(di.payload(Object.class).hashCode());
        rear.tell(di, context.self());
      });
    });
    invalidatingBucket.clearRange(0, pos);

    outDocIds.forEach(value -> {
      tracer.log(value, System.nanoTime() - docFlush[value]);
      return true;
    });
  }

  @Override
  public void onStop() throws Exception {
    try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(
            Paths.get("/tmp/elements.cnt"),
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.CREATE
    ))) {
      pw.println(allItems + "/" + validItems);
    }
  }
}
