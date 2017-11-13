package com.spbsu.flamestream.runtime.ack;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.ack.impl.ArrayAckTable;
import com.spbsu.flamestream.runtime.ack.messages.*;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.range.HashRange;
import com.spbsu.flamestream.runtime.tick.StartTick;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import com.spbsu.flamestream.runtime.tick.TickRoutes;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.HashSet;

public final class AckActor extends LoggingActor {
  private final TIntObjectMap<AckTable> tables = new TIntObjectHashMap<>();
  private final AckerStatistics stat = new AckerStatistics();
  private final Collection<HashRange> committers = new HashSet<>();
  private GlobalTime currentMin = GlobalTime.MIN;

  private final TickInfo tickInfo;
  private final ActorRef tickWatcher;

  @Nullable
  private TickRoutes tickRoutes = null;

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
    return ReceiveBuilder.create().match(StartTick.class, start -> {
      log().info("Received start tick");
      tickRoutes = start.tickRoutingInfo();
      unstashAll();
      getContext().become(acking());
    }).matchAny(m -> stash()).build();
  }

  private Receive acking() {
    return ReceiveBuilder.create()
            .match(Ack.class, this::handleAck)
            .build();
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

    final boolean report = ack.isReport();
    if (report) {
      log().debug("Front report received: {}", ack);
      ackTable.report(time);
    }

    final long start = System.nanoTime();
    //assertMonotonicAck(ack.time());
    final boolean nullified = ackTable.ack(time, ack.xor());
    if (nullified || report) {
      checkMinTime();
      stat.recordReleasingAck(System.nanoTime() - start);
    } else {
      stat.recordNormalAck(System.nanoTime() - start);
    }
  }

  private void checkMinTime() {
    final GlobalTime minAmongTables = minAmongTables();
    if (minAmongTables.compareTo(this.currentMin) > 0) {
      this.currentMin = minAmongTables;
      { //send min updates
        log().debug("New min time: {}", this.currentMin);
        //noinspection ConstantConditions
        tickRoutes.rangeConcierges().values().forEach(r -> r.tell(new MinTimeUpdate(this.currentMin), self()));
      }
    }

    if (minAmongTables.time() == tickInfo.stopTs()) {
      { //send commit
        log().info("Committing");
        //noinspection ConstantConditions
        tickRoutes.rangeConcierges().values().forEach(r -> r.tell(new Commit(), self()));
      }
      getContext().become(ReceiveBuilder.create().match(RangeCommitDone.class, rangeCommitDone -> {
        log().debug("Received: {}", rangeCommitDone);
        final HashRange committer = rangeCommitDone.committer();
        committers.add(committer);
        if (committers.equals(tickInfo.hashMapping().keySet())) {
          log().info("Tick commit done");
          tickWatcher.tell(new CommitTick(tickInfo.id()), self());
          context().stop(self());
        }
      }).build());
    } else if (minAmongTables.time() > tickInfo.stopTs()) {
      throw new IllegalStateException("Ledger min must be less or equal to tick stop ts");
    }
  }

  private GlobalTime minAmongTables() {
    final int[] frontMin = {Integer.MAX_VALUE};
    final long[] timeMin = {Long.MAX_VALUE};
    tables.forEachEntry((f, table) -> {
      final long tmpMin = table.min();
      if (tmpMin < timeMin[0] || tmpMin == timeMin[0] && f < frontMin[0]) {
        frontMin[0] = f;
        timeMin[0] = tmpMin;
      }
      return true;
    });
    return new GlobalTime(timeMin[0], frontMin[0]);
  }

  @SuppressWarnings("unused")
  private void assertMonotonicAck(GlobalTime newTime) {
    if (newTime.compareTo(currentMin) < 0) {
      throw new IllegalStateException("Not monotonic acks. Fixme");
    }
  }
}
