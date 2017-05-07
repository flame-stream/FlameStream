package com.spbsu.datastream.core.ack;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.range.AddressedMessage;
import com.spbsu.datastream.core.tick.TickContext;

public final class AckActor extends LoggingActor {
  private final AckLedger ledger;
  private final TickContext context;
  private GlobalTime currentMin = GlobalTime.MIN;

  private AckActor(final TickContext context) {
    this.ledger = new AckLedgerImpl(context.tickInfo().startTs(),
            context.tickInfo().window(),
            context.tickInfo().graph().frontBindings().keySet());
    this.context = context;

    this.LOG().info("Acker initiated: startTs:{}, window: {}, fronts: {}", this.ledger.startTs(), this.ledger.window(), this.ledger.initHashes());
  }

  public static Props props(final TickContext context) {
    return Props.create(AckActor.class, context);
  }

  @Override
  public void onReceive(final Object message) {
    if (message instanceof FrontReport) {
      final FrontReport report = (FrontReport) message;
      this.LOG().debug("Front report received: {}", report);
      this.ledger.report(report.globalTime(), report.xor());
    } else if (message instanceof Ack) {
      final Ack ack = (Ack) message;
      this.ledger.ack(ack.time(), ack.xor());
      this.LOG().debug("Ack received: {}", ack);
    } else {
      throw new IllegalArgumentException("Unexpected message type");
    }

    final GlobalTime ledgerMin = this.ledger.min();
    if (ledgerMin.compareTo(this.currentMin) > 0) {
      this.currentMin = ledgerMin;
      this.sendMinUpdates(this.currentMin);
    }
  }

  private void sendMinUpdates(final GlobalTime min) {
    this.LOG().debug("New min time: {}", min);
    this.context.rootRouter().tell(new AddressedMessage<>(this.context.tickInfo().startTs(), new MinTimeUpdate(min)), ActorRef.noSender());
  }
}
