package com.spbsu.datastream.core.feedback;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.tick.TickContext;

import java.util.ArrayList;
import java.util.List;

public final class AckActor extends LoggingActor {
  private final AckLedger ledger;
  private GlobalTime currentMin = GlobalTime.INF;

  private final List<ActorRef> resultSubscribers;

  public static Props props(final TickContext context, final List<ActorRef> resultSubscribers) {
    return Props.create(AckActor.class, context, resultSubscribers);
  }

  private AckActor(final TickContext context, final List<ActorRef> resultSubscribers) {
    this.resultSubscribers = new ArrayList<>(resultSubscribers);
    this.ledger = new AckLedgerImpl(context.startTime(), context.window(), null);
  }

  @Override
  public void onReceive(final Object message) {
    if (message instanceof FrontReport) {
      final FrontReport report = (FrontReport) message;
      this.ledger.report(report.globalTime());
      this.ledger.ack(report.globalTime(), report.xor());
    } else if (message instanceof Ack) {
      final Ack ack = (Ack) message;
      this.ledger.ack(ack.time(), ack.xor());
    } else {
      throw new IllegalArgumentException("Unexpected message type");
    }

    final GlobalTime ledgerMin = this.ledger.min();
    if (ledgerMin.compareTo(this.currentMin) < 0) {
      this.currentMin = ledgerMin;
      this.sendMinUpdates(this.currentMin);
    }
  }

  private void sendMinUpdates(final GlobalTime min) {
    // TODO: 4/25/17  
  }
}
