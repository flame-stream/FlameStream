package com.spbsu.datastream.core.ack;

import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.impl.AckLedgerImpl;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.meta.GlobalTime;
import com.spbsu.datastream.core.stat.AckerStatistics;
import com.spbsu.datastream.core.tick.TickRoutes;
import com.spbsu.datastream.core.tick.StartTick;
import com.spbsu.datastream.core.tick.TickInfo;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.HashSet;

public final class AckActor extends LoggingActor {
  private final AckLedger ledger;
  private final TickInfo tickInfo;
  private GlobalTime currentMin = GlobalTime.MIN;

  @Nullable
  private TickRoutes tickRoutes;

  private final AckerStatistics stat = new AckerStatistics();

  private final Collection<HashRange> committers = new HashSet<>();

  private AckActor(TickInfo tickInfo) {
    this.ledger = new AckLedgerImpl(tickInfo);
    this.tickInfo = tickInfo;
  }

  public static Props props(TickInfo tickInfo) {
    return Props.create(AckActor.class, tickInfo);
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(StartTick.class, start -> {
              LOG().info("Received start tick");
              tickRoutes = start.tickRoutingInfo();
              getContext().become(acking());
            })
            .build();
  }

  private Receive acking() {
    return receiveBuilder()
            .match(AckerReport.class, this::handleReport)
            .match(Ack.class, this::handleAck)
            .build();
  }

  @Override
  public void postStop() {
    super.postStop();
    LOG().info("Acker statistics: {}", stat);

    LOG().debug("Acker ledger: {}", ledger);
  }

  private void handleReport(AckerReport report) {
    LOG().debug("Front report received: {}", report);
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

    if (ledgerMin.time() >= tickInfo.stopTs()) {
      sendCommit();
      getContext().become(receiveBuilder().match(CommitDone.class, this::handleDone).build());
    }
  }

  private void assertMonotonicAck(GlobalTime newTime) {
    if (newTime.compareTo(currentMin) < 0) {
      throw new IllegalStateException("Not monotonic acks. Fixme");
    }
  }

  private void handleDone(CommitDone commitDone) {
    LOG().debug("Received: {}", commitDone);
    final HashRange committer = commitDone.committer();
    committers.add(committer);
    if (committers.equals(tickInfo.hashMapping().keySet())) {
      LOG().info("COOOOMMMMITTTITITITITITI");
    }
  }

  private void sendCommit() {
    LOG().info("Committing");
    tickRoutes.rangeConcierges().values().forEach(r -> r.tell(new Commit(), self()));
  }

  private void sendMinUpdates(GlobalTime min) {
    LOG().debug("New min time: {}", min);
    tickRoutes.rangeConcierges().values().forEach(r -> r.tell(new MinTimeUpdate(min), self()));
  }
}
