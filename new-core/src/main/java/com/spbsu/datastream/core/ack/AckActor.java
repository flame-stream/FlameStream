package com.spbsu.datastream.core.ack;

import akka.actor.Props;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.tick.TickContext;

import java.util.Collections;

public final class AckActor extends LoggingActor {
  private final AckLedger ledger;
  private GlobalTime currentMin = GlobalTime.INF;

  public static Props props(final TickContext context) {
    return Props.create(AckActor.class, context);
  }

  private AckActor(final TickContext context) {
    this.ledger = new AckLedgerImpl(context.startTime(), context.window(), Collections.emptySet());
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
