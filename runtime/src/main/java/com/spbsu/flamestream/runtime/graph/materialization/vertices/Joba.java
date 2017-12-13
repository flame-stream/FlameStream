package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import akka.actor.ActorContext;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.graph.materialization.Materializer;
import com.spbsu.flamestream.runtime.graph.materialization.Router;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
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
    private final Consumer<DataItem> acker;

    Stub(Joba[] outJobas, Consumer<DataItem> acker) {
      this.outJobas = outJobas;
      this.acker = acker;
    }

    void process(DataItem input, Stream<DataItem> output, boolean fromAsync) {
      // TODO: 12.12.2017 everything is ready for optimization
      output.forEach(dataItem -> {
        if (outJobas.length == 1) {
          sendToNext(outJobas[0], dataItem);
        } else if (outJobas.length > 1) { //broadcast
          for (int i = 0; i < outJobas.length; i++) {
            final Meta newMeta = dataItem.meta().advanced(BROADCAST_LOCAL_TIME, i);
            final DataItem newItem = new BroadcastDataItem(dataItem, newMeta);
            sendToNext(outJobas[i], newItem);
          }
        }
      });
      if (fromAsync) {
        //ACK for input DI should be later than for output
        acker.accept(input);
      }
    }

    private void sendToNext(Joba next, DataItem dataItem) {
      next.accept(dataItem, isAsync());
      if (next.isAsync()) {
        acker.accept(dataItem);
      }
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

  class CachedBuilder {
    private final Collection<CachedBuilder> output = new ArrayList<>();
    private final Graph.Vertex vertex;

    private boolean async = false;
    private Joba cachedJoba = null;

    public CachedBuilder(Graph.Vertex vertex) {
      this.vertex = vertex;
    }

    public CachedBuilder addOutput(CachedBuilder builder) {
      if (builder != null) {
        output.add(builder);
      }
      return this;
    }

    public CachedBuilder async() {
      async = true;
      return this;
    }

    public Joba build(Consumer<DataItem> acker,
                      Consumer<GlobalTime> heartBeater,
                      Consumer<DataItem> barrier,
                      IntRangeMap<Router> routers,
                      ActorContext context) {
      if (cachedJoba != null) {
        return cachedJoba;
      } else {
        final Joba[] jobas = output.stream()
                .map(builder -> {
                  if (builder.vertex instanceof Grouping) {
                    return new RouterJoba(routers, ((Grouping) builder.vertex).hash(), new Materializer.Destination(builder.vertex.id()));
                  }
                  return builder.build(acker, heartBeater, barrier, routers, context);
                })
                .toArray(Joba[]::new);
        if (vertex instanceof Sink) {
          cachedJoba = new SinkJoba(jobas, acker, barrier);
        } else if (vertex instanceof FlameMap) {
          cachedJoba = new MapJoba(jobas, acker, (FlameMap<?, ?>) vertex);
        } else if (vertex instanceof Grouping) {
          cachedJoba = new GroupingJoba(jobas, acker, (Grouping) vertex);
        } else if (vertex instanceof Source) {
          cachedJoba = new SourceJoba(jobas, acker, 10, context, heartBeater);
        } else {
          throw new RuntimeException("Invalid vertex type");
        }

        /*if (async) {
          cachedJoba = new ActorJoba(cachedJoba, context);
        }*/
        return cachedJoba;
      }
    }
  }
}
