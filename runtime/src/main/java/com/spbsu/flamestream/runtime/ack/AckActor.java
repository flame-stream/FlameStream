package com.spbsu.flamestream.runtime.ack;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.ack.impl.AckLedgerImpl;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.range.HashRange;
import com.spbsu.flamestream.runtime.tick.StartTick;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import com.spbsu.flamestream.runtime.tick.TickRoutes;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.HashSet;

public final class AckActor extends LoggingActor {
  private final AckLedger ledger;
  private final TickInfo tickInfo;
  private final ActorRef tickWatcher;
  private final AckerStatistics stat = new AckerStatistics();
  private final Collection<HashRange> committers = new HashSet<>();
  private GlobalTime currentMin = GlobalTime.MIN;
  @Nullable
  private TickRoutes tickRoutes = null;

  private AckActor(TickInfo tickInfo, ActorRef tickWatcher) {
    this.ledger = new AckLedgerImpl(tickInfo);
    this.tickInfo = tickInfo;
    this.tickWatcher = tickWatcher;
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
            .match(AckerReport.class, this::handleReport)
            .match(Ack.class, this::handleAck)
            .build();
  }

  @Override
  public void postStop() {
    super.postStop();
    log().info("Acker statistics: {}", stat);

    log().debug("Acker ledger: {}", ledger);
  }

  private void handleReport(AckerReport report) {
    log().debug("Front report received: {}", report);
    ledger.report(report.globalTime(), report.xor());
    checkLedgerTime();
  }

  private void handleAck(Ack ack) {
    final long start = System.nanoTime();
    //assertMonotonicAck(ack.time());

    if (ledger.ack(ack.time(), ack.xor())) {
      checkLedgerTime();
      stat.recordReleasingAck(System.nanoTime() - start);
    } else {
      stat.recordNormalAck(System.nanoTime() - start);
    }
  }

  private void checkLedgerTime() {
    final GlobalTime ledgerMin = ledger.min();
    if (ledgerMin.compareTo(currentMin) > 0) {
      this.currentMin = ledgerMin;
      sendMinUpdates(currentMin);
    }

    if (ledgerMin.time() == tickInfo.stopTs()) {
      sendCommit();
      getContext().become(ReceiveBuilder.create().match(RangeCommitDone.class, this::handleDone).build());
    } else if (ledgerMin.time() > tickInfo.stopTs()) {
      throw new IllegalStateException("Ledger min must be less or equal to tick stop ts");
    }
  }

  private void assertMonotonicAck(GlobalTime newTime) {
    if (newTime.compareTo(currentMin) < 0) {
      throw new IllegalStateException("Not monotonic acks. Fixme");
    }
  }

  private void handleDone(RangeCommitDone rangeCommitDone) {
    log().debug("Received: {}", rangeCommitDone);

    final HashRange committer = rangeCommitDone.committer();
    committers.add(committer);
    if (committers.equals(tickInfo.hashMapping().keySet())) {
      log().info("Tick commit done");
      tickWatcher.tell(new CommitTick(tickInfo.id()), self());
      context().stop(self());
    }
  }

  private void sendCommit() {
    log().info("Committing");
    tickRoutes.rangeConcierges().values().forEach(r -> r.tell(new Commit(), self()));
  }

  private void sendMinUpdates(GlobalTime min) {
    log().debug("New min time: {}", min);
    tickRoutes.rangeConcierges().values().forEach(r -> r.tell(new MinTimeUpdate(min), self()));
  }
}
