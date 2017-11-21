package com.spbsu.flamestream.runtime.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.utils.Statistics;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.Commit;
import com.spbsu.flamestream.runtime.acker.api.CommitTick;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.acker.api.RangeCommitDone;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.node.tick.range.atomic.source.api.Heartbeat;
import com.spbsu.flamestream.runtime.node.tick.range.HashRange;
import com.spbsu.flamestream.runtime.node.tick.api.StartTick;
import com.spbsu.flamestream.runtime.node.tick.api.TickInfo;
import com.spbsu.flamestream.runtime.node.tick.api.TickRoutes;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LongSummaryStatistics;
import java.util.Map;

public class AckActor extends LoggingActor {
  private final TickInfo tickInfo;
  private final ActorRef tickWatcher;
  private TickRoutes tickRoutes = null;

  private final Map<String, AckTable> tables = new HashMap<>();
  private final AckerStatistics stat = new AckerStatistics();
  private final Collection<HashRange> committers = new HashSet<>();

  private GlobalTime currentMin = GlobalTime.MIN;

  private AckActor(TickInfo tickInfo, ActorRef tickWatcher) {
    this.tickInfo = tickInfo;
    this.tickWatcher = tickWatcher;
    tickInfo.fronts()
            .forEach(i -> tables.put(i, new ArrayAckTable(tickInfo.startTs(), tickInfo.stopTs(), tickInfo.window())));
  }

  public static Props props(TickInfo tickInfo, ActorRef tickWatcher) {
    return Props.create(AckActor.class, tickInfo, tickWatcher);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(StartTick.class, start -> {
              log().info("Received start tick");
              tickRoutes = start.tickRoutingInfo();
              unstashAll();
              getContext().become(acking());
            })
            .matchAny(m -> stash())
            .build();
  }

  private Receive acking() {
    return ReceiveBuilder.create()
            .match(Ack.class, this::handleAck)
            .match(Heartbeat.class, this::handleHeartBeat)
            .build();
  }

  private void handleHeartBeat(Heartbeat heartbeat) {
    final GlobalTime time = heartbeat.time();
    tables.get(time.front()).heartbeat(time.time());
    checkMinTime();
  }

  @Override
  public void postStop() {
    super.postStop();
    log().info("Acker statistics: {}", stat);
    log().debug("Acker tables: {}", tables);
  }

  private void handleAck(Ack ack) {
    final GlobalTime globalTime = ack.time();
    final AckTable ackTable = tables.get(globalTime.front());
    final long time = globalTime.time();

    final long start = System.nanoTime();
    if (ackTable.ack(time, ack.xor())) {
      checkMinTime();
      stat.recordReleasingAck(System.nanoTime() - start);
    } else {
      stat.recordNormalAck(System.nanoTime() - start);
    }
  }

  private void checkMinTime() {
    final GlobalTime minAmongTables = minAmongTables();
    if (minAmongTables.compareTo(currentMin) > 0) {
      this.currentMin = minAmongTables;
      { //send min updates
        log().debug("New min time: {}", this.currentMin);
        //noinspection ConstantConditions
        tickRoutes.rangeConcierges().values().forEach(r -> r.tell(new MinTimeUpdate(this.currentMin), self()));
      }
    }

    if (minAmongTables.time() >= tickInfo.stopTs()) {
      log().info("Committing");
      tickRoutes.rangeConcierges().values().forEach(r -> r.tell(new Commit(), self()));

      getContext().become(ReceiveBuilder.create()
              .match(RangeCommitDone.class, rangeCommitDone -> {
                log().debug("Received: {}", rangeCommitDone);
                final HashRange committer = rangeCommitDone.committer();
                committers.add(committer);
                if (committers.equals(tickInfo.hashMapping().keySet())) {
                  log().info("Tick commit done");
                  tickWatcher.tell(new CommitTick(tickInfo.id()), self());
                  context().stop(self());
                }
              })
              .match(Heartbeat.class, heartbeat -> {
              })
              .build());
    }
  }

  private GlobalTime minAmongTables() {
    final String[] frontMin = {"Hi"};
    final long[] timeMin = {Long.MAX_VALUE};
    tables.forEach((f, table) -> {
      final long tmpMin = table.min();
      if (tmpMin < timeMin[0] || tmpMin == timeMin[0] && f.compareTo(frontMin[0]) < 0) {
        frontMin[0] = f;
        timeMin[0] = tmpMin;
      }
    });
    return new GlobalTime(timeMin[0], frontMin[0]);
  }

  private static final class AckerStatistics implements Statistics {
    private final LongSummaryStatistics normalAck = new LongSummaryStatistics();
    private final LongSummaryStatistics releasingAck = new LongSummaryStatistics();

    void recordNormalAck(long ts) {
      normalAck.accept(ts);
    }

    void recordReleasingAck(long ts) {
      releasingAck.accept(ts);
    }

    @Override
    public Map<String, Double> metrics() {
      final Map<String, Double> result = new HashMap<>();
      result.putAll(Statistics.asMap("Normal ack duration", normalAck));
      result.putAll(Statistics.asMap("Releasing ack duration", releasingAck));
      return result;
    }

    @Override
    public String toString() {
      return metrics().toString();
    }
  }
}
