package com.spbsu.datastream.core.ack;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.node.UnresolvedMessage;
import com.spbsu.datastream.core.range.HashedMessage;
import com.spbsu.datastream.core.tick.TickInfo;
import com.spbsu.datastream.core.tick.TickMessage;

import java.util.Collection;
import java.util.HashSet;

public final class AckActor extends LoggingActor {
  private final AckLedger ledger;
  private final TickInfo tickInfo;
  private final ActorRef dns;
  private GlobalTime currentMin = GlobalTime.MIN;

  private final Collection<HashRange> committers = new HashSet<>();

  private AckActor(final TickInfo tickInfo, final ActorRef dns) {
    this.ledger = new AckLedgerImpl(tickInfo.startTs(),
            tickInfo.window(),
            tickInfo.graph().frontBindings().keySet());
    this.tickInfo = tickInfo;
    this.dns = dns;

    this.LOG().info("Acker initiated: startTs:{}, window: {}, fronts: {}", this.ledger.startTs(), this.ledger.window(), this.ledger.initHashes());
  }

  public static Props props(final TickInfo tickInfo, final ActorRef dns) {
    return Props.create(AckActor.class, tickInfo, dns);
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder()
            .match(AckerReport.class, this::handleReport)
            .match(Ack.class, this::handleAck)
            .build();
  }

  private void handleReport(final AckerReport report) {
    this.LOG().debug("Front report received: {}", report);
    this.ledger.report(report.globalTime(), report.xor());
    this.checkLedgerTime();
  }

  private void handleAck(final Ack ack) {
    this.LOG().debug("Ack received: {}", ack);
    this.ledger.ack(ack.time(), ack.xor());
    this.checkLedgerTime();
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

  private void handleDone(final CommitDone commitDone) {
    this.LOG().debug("Received: {}", commitDone);
    final HashRange committer = commitDone.committer();
    this.committers.add(committer);
    if (this.committers.equals(this.tickInfo.hashMapping().keySet())) {
      this.LOG().info("COOOOMMMMITTTITITITITITI");
    }
  }

  private void sendCommit() {
    this.LOG().info("Committing");
    this.dns.tell(new UnresolvedMessage<>(new TickMessage<>(this.tickInfo.startTs(), new HashedMessage<>(new Commit()))), ActorRef.noSender());
  }

  private void sendMinUpdates(final GlobalTime min) {
    this.LOG().debug("New min time: {}", min);
    this.dns.tell(new UnresolvedMessage<>(new TickMessage<>(this.tickInfo.startTs(), new HashedMessage<>(new MinTimeUpdate(min)))), ActorRef.noSender());
  }
}
