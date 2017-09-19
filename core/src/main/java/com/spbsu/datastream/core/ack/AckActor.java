package com.spbsu.datastream.core.ack;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.BroadcastMessage;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.impl.AckLedgerImpl;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.node.UnresolvedMessage;
import com.spbsu.datastream.core.tick.TickInfo;

import java.util.Collection;
import java.util.HashSet;

public final class AckActor extends LoggingActor {
  private final AckLedger ledger;
  private final TickInfo tickInfo;
  private final ActorRef dns;
  private GlobalTime currentMin = GlobalTime.MIN;

  private final Collection<HashRange> committers = new HashSet<>();

  private AckActor(TickInfo tickInfo, ActorRef dns) {
    this.ledger = new AckLedgerImpl(tickInfo);
    this.tickInfo = tickInfo;
    this.dns = dns;
  }

  public static Props props(TickInfo tickInfo, ActorRef dns) {
    return Props.create(AckActor.class, tickInfo, dns);
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder()
            .match(AckerReport.class, this::handleReport)
            .match(Ack.class, this::handleAck)
            .build();
  }

  @Override
  public void postStop() throws Exception {
    super.postStop();

    this.LOG().debug("Acker ledger: {}", this.ledger);
  }

  private void handleReport(AckerReport report) {
    this.LOG().debug("Front report received: {}", report);
    this.ledger.report(report.globalTime(), report.xor());
    this.checkLedgerTime();
  }

  private void handleAck(Ack ack) {
    this.assertMonotonicAck(ack.time());

    this.LOG().debug("Ack received: {}", ack);
    if (this.ledger.ack(ack.time(), ack.xor())) {
      this.checkLedgerTime();
    }
  }

  private void checkLedgerTime() {
    final GlobalTime ledgerMin = this.ledger.min();
    if (ledgerMin.compareTo(this.currentMin) > 0) {
      this.currentMin = ledgerMin;
      this.sendMinUpdates(this.currentMin);
    }

    if (ledgerMin.time() >= this.tickInfo.stopTs()) {
      this.sendCommit();
      this.getContext().become(this.receiveBuilder().match(CommitDone.class, this::handleDone).build());
    }
  }

  private void assertMonotonicAck(GlobalTime newTime) {
    if (newTime.compareTo(this.currentMin) < 0) {
      throw new IllegalStateException("Not monotonic acks. Fixme");
    }
  }

  private void handleDone(CommitDone commitDone) {
    this.LOG().debug("Received: {}", commitDone);
    final HashRange committer = commitDone.committer();
    this.committers.add(committer);
    if (this.committers.equals(this.tickInfo.hashMapping().asMap().keySet())) {
      this.LOG().info("COOOOMMMMITTTITITITITITI");
    }
  }

  private void sendCommit() {
    this.LOG().info("Committing");
    this.dns.tell(new UnresolvedMessage<>(new BroadcastMessage<>(new Commit(), this.tickInfo.startTs())), this.self());
  }

  private void sendMinUpdates(GlobalTime min) {
    this.LOG().debug("New min time: {}", min);
    this.dns.tell(new UnresolvedMessage<>(new BroadcastMessage<>(new MinTimeUpdate(min), this.tickInfo.startTs())), this.self());
  }
}
