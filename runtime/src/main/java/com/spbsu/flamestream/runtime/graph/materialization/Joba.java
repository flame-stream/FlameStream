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
    private final Joba[] outJobas;

    protected final ActorContext context;
    protected final ActorRef acker;

    Stub(Stream<Joba> output, ActorRef acker, ActorContext context) {
      this.outJobas = output.toArray(Joba[]::new);
      this.acker = acker;
      this.context = context;
    }

    void process(DataItem input, Stream<DataItem> output, boolean fromAsync) {
      output.forEach(dataItem -> {
        if (outJobas.length == 1) {
          sendToNext(outJobas[0], dataItem);
        } else if (outJobas.length > 1) { //broadcast
          for (int i = 0; i < outJobas.length; i++) {
            final Meta newMeta = new Meta(dataItem.meta(), 0, i);
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

    private void ack(DataItem item) {
      acker.tell(new Ack(item.meta().globalTime(), item.xor()), context.self());
    }

    // TODO: 13.12.2017 i believe there is more effective and smart solution
    private static class BroadcastDataItem implements DataItem {
      private final DataItem inner;
      private final Meta newMeta;
      private final long xor;

      private BroadcastDataItem(DataItem inner, Meta newMeta) {
        this.inner = inner;
        this.newMeta = newMeta;
        this.xor = ThreadLocalRandom.current().nextLong();
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
        return xor;
      }
    }
  }
}
