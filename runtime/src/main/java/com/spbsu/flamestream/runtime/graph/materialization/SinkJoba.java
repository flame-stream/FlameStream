package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

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

  private final Tracing.Tracer receiveTracer = Tracing.TRACING.forEvent("barrier-receive", 850000, 1);
  private final Tracing.Tracer sendTracer = Tracing.TRACING.forEvent("barrier-send", 850000, 1);

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
    receiveTracer.log(dataItem.payload(Object.class).hashCode());
    allItems++;
    //rears.forEach(rear -> rear.tell(dataItem, context.self()));
    invalidatingBucket.insert(dataItem);
    process(dataItem, Stream.of(dataItem), fromAsync);
  }

  @Override
  public void onMinTime(GlobalTime minTime) {
    final int pos = invalidatingBucket.lowerBound(new Meta(minTime));
    invalidatingBucket.rangeStream(0, pos).forEach(di -> {
      validItems++;
      rears.forEach(rear -> {
        sendTracer.log(di.payload(Object.class).hashCode());
        rear.tell(di, context.self());
      });
    });
    invalidatingBucket.clearRange(0, pos);
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
