package com.spbsu.datastream.core.range.atomic;

import akka.actor.ActorContext;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.google.common.primitives.Longs;
import com.spbsu.datastream.core.AckerMessage;
import com.spbsu.datastream.core.AtomicMessage;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.RoutingException;
import com.spbsu.datastream.core.ack.Ack;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.node.UnresolvedMessage;
import com.spbsu.datastream.core.stat.Statistics;
import com.spbsu.datastream.core.tick.TickInfo;
import org.iq80.leveldb.DB;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;
import java.util.function.ToIntFunction;

public final class AtomicHandleImpl implements AtomicHandle {
  private final TickInfo tickInfo;
  private final ActorRef dns;
  private final DB db;
  private final ActorContext context;
  private final LoggingAdapter LOG;

  public AtomicHandleImpl(TickInfo tickInfo,
                          ActorRef dns,
                          DB db,
                          ActorContext context) {
    this.tickInfo = tickInfo;
    this.dns = dns;
    this.db = db;
    this.context = context;
    LOG = Logging.getLogger(context.system(), context.self());
  }

  @Override
  public ActorSelection actorSelection(ActorPath path) {
    return this.context.actorSelection(path);
  }

  @Override
  public void push(OutPort out, DataItem<?> result) {
    final InPort destination = this.tickInfo.graph().graph().downstreams().get(out);
    if (destination == null) throw new RoutingException("Unable to find port for " + out);

    @SuppressWarnings("rawtypes") final ToIntFunction hashFunction = destination.hashFunction();
    @SuppressWarnings("unchecked") final int hash = hashFunction.applyAsInt(result.payload());
    final int receiver = this.tickInfo.hashMapping().workerForHash(hash);

    final UnresolvedMessage<AtomicMessage<?>> message = new UnresolvedMessage<>(
            receiver,
            new AtomicMessage<>(this.tickInfo.startTs(), hash, destination, result)
    );

    this.ack(result);
    this.dns.tell(message, this.context.self());
  }

  @Override
  public void ack(DataItem<?> item) {
    final int id = this.tickInfo.ackerLocation();

    final UnresolvedMessage<AckerMessage<?>> message = new UnresolvedMessage<>(id,
            new AckerMessage<>(new Ack(item.ack(), item.meta().globalTime()), this.tickInfo.startTs()));
    this.dns.tell(message, this.context.self());
  }

  @Override
  public Optional<Object> loadState(InPort inPort) {
    final byte[] key = Longs.toByteArray(inPort.id());
    final byte[] value = this.db.get(key);
    if (value != null) {
      final ByteArrayInputStream in = new ByteArrayInputStream(value);
      try {
        final ObjectInputStream is = new ObjectInputStream(in);
        final Object state = is.readObject();
        is.close();
        in.close();
        return Optional.of(state);
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    } else {
      return Optional.empty();
    }
  }

  @Override
  public void saveState(InPort inPort, Object state) {
    final byte[] key = Longs.toByteArray(inPort.id());
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      final ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(state);
      oos.close();

      final byte[] value = bos.toByteArray();
      bos.close();
      this.db.put(key, value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeState(InPort inPort) {
    final byte[] key = Longs.toByteArray(inPort.id());
    this.db.delete(key);
  }

  @Override
  public void submitStatistics(Statistics stat) {
    LOG.info("Inner statistics: {}", stat);
  }

  @Override
  public TickInfo tickInfo() {
    return tickInfo;
  }

  @Override
  public void error(String format, Object... args) {
    LOG.warning(format, args);
  }
}
