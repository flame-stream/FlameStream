package com.spbsu.datastream.core.front;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.range.AddressedMessage;
import com.spbsu.datastream.core.tick.PortBindDataItem;

public final class TickFrontActor extends LoggingActor {

  private final ActorRef rootRouter;
  private final InPort target;

  private final long tick;

  public static Props props(final ActorRef rootRouter, final InPort target, final long tick) {
    return Props.create(TickFrontActor.class, rootRouter, target, tick);
  }

  private TickFrontActor(final ActorRef rootRouter, final InPort target, final long tick) {
    this.rootRouter = rootRouter;
    this.target = target;
    this.tick = tick;
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof DataItem) {
      final DataItem<?> item = (DataItem<?>) message;

      @SuppressWarnings("rawtypes")
      final HashFunction hashFunction = this.target.hashFunction();

      @SuppressWarnings("unchecked")
      final int hash = hashFunction.applyAsInt(item.payload());

      // TODO: 4/28/17 bookkeeping
      this.rootRouter.tell(new AddressedMessage<>(new PortBindDataItem(item, this.target), hash, this.tick),
              ActorRef.noSender());
    }
  }
}
