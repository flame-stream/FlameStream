package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class SinkJoba implements Joba {
  private final InvalidatingBucket invalidatingBucket = new ArrayInvalidatingBucket();
  private final List<ActorRef> rears = new ArrayList<>();
  private final ActorContext context;

  private final Tracing.Tracer sinkReceive = Tracing.TRACING.forEvent("sink-receive");
  private final Tracing.Tracer sinkSend = Tracing.TRACING.forEvent("sink-send");

  private int cameToBarrier = 0;
  private int releasedFromBarrier = 0;

  public SinkJoba(ActorContext context) {
    this.context = context;
  }

  @Override
  public void accept(DataItem item, Consumer<DataItem> sink) {
    cameToBarrier++;
    sinkReceive.log(item.xor());
    //rears.forEach(rear -> rear.tell(dataItem, context.self()));
    invalidatingBucket.insert(item);
  }

  public void attachRear(ActorRef rear) {
    rears.add(rear);
  }

  @Override
  public void onMinTime(GlobalTime minTime) {
    final int pos = invalidatingBucket.lowerBound(new Meta(minTime));
    invalidatingBucket.rangeStream(0, pos).forEach(di -> {
      releasedFromBarrier++;
      sinkSend.log(di.xor());
      rears.forEach(rear -> {
        rear.tell(di, context.self());
      });
    });
    invalidatingBucket.clearRange(0, pos);
  }

  @Override
  public void postStop() {
    try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(Paths.get("/tmp/elements.cnt")))) {
      pw.println(cameToBarrier + "/" + releasedFromBarrier);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
