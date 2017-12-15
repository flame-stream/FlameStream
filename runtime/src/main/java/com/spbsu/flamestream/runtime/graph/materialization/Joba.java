package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.acker.api.Ack;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public interface Joba {
  boolean isAsync();

  void accept(DataItem dataItem, boolean fromAsync);

  abstract class Stub implements Joba {
    private static final int BROADCAST_LOCAL_TIME = Integer.MAX_VALUE;
    private final Joba[] outJobas;

    protected final ActorContext context;
    protected final ActorRef acker;

    Stub(Stream<Joba> output, ActorRef acker, ActorContext context) {
      this.outJobas = output.toArray(Joba[]::new);
      this.acker = acker;
      this.context = context;
    }

    void process(DataItem input, Stream<DataItem> output, boolean fromAsync) {
      // TODO: 12.12.2017 everything is ready for optimization
      output.forEach(dataItem -> {
        if (outJobas.length == 1) {
          sendToNext(outJobas[0], dataItem);
        } else if (outJobas.length > 1) { //broadcast
          for (int i = 0; i < outJobas.length; i++) {
            final Meta newMeta = new Meta(dataItem.meta(), BROADCAST_LOCAL_TIME, i);
            final DataItem newItem = new BroadcastDataItem(dataItem, newMeta);
            sendToNext(outJobas[i], newItem);
          }
        }
      });
      if (fromAsync) {
        //ACK for input DI should be later than for output
        ack(input);
      }
    }

    private void sendToNext(Joba next, DataItem dataItem) {
      next.accept(dataItem, isAsync());
      if (next.isAsync()) {
        ack(dataItem);
      }
    }

    private void ack(DataItem dataItem) {
      acker.tell(new Ack(dataItem.meta().globalTime(), dataItem.xor()), context.self());
    }

    // TODO: 13.12.2017 i believe there is more effective and smart solution
    private static class BroadcastDataItem implements DataItem {
      private final DataItem inner;
      private final Meta newMeta;
      private final long ackHashCode;

      private BroadcastDataItem(DataItem inner, Meta newMeta) {
        this.inner = inner;
        this.newMeta = newMeta;
        ackHashCode = ThreadLocalRandom.current().nextLong();
      }

      @Override
      public Meta meta() {
        return newMeta;
      }

      @Override
      public <T> T payload(Class<T> expectedClass) {
        return inner.payload(expectedClass);
      }

      @Override
      public long xor() {
        return ackHashCode;
      }
    }
  }
}
