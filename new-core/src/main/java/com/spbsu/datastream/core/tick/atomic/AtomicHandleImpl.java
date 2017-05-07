package com.spbsu.datastream.core.tick.atomic;

import akka.actor.ActorContext;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import com.google.common.primitives.Longs;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.RoutingException;
import com.spbsu.datastream.core.ack.Ack;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.AddressedMessage;
import com.spbsu.datastream.core.tick.PortBindDataItem;
import com.spbsu.datastream.core.tick.TickContext;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.DbImpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;

public final class AtomicHandleImpl implements AtomicHandle {
  private final TickContext tickContext;
  private final ActorContext context;
  private final DB db;

  public AtomicHandleImpl(final DB db, final TickContext tickContext, final ActorContext context) {
    this.tickContext = tickContext;
    this.context = context;
    this.db = db;
  }

  @Override
  public ActorSelection actorSelection(final ActorPath path) {
    return this.context.actorSelection(path);
  }

  @Override
  public void push(final OutPort out, final DataItem<?> result) {
    final Optional<InPort> destination = Optional.ofNullable(this.tickContext.tickInfo().graph().graph().downstreams().get(out));
    final InPort address = destination.orElseThrow(() -> new RoutingException("Unable to find port for " + out));

    @SuppressWarnings("rawtypes") final HashFunction hashFunction = address.hashFunction();

    @SuppressWarnings("unchecked") final int hash = hashFunction.applyAsInt(result.payload());

    final AddressedMessage<?> addressedMessage = new AddressedMessage<>(new PortBindDataItem(result, address), hash, this.tickContext.tickInfo().startTs());
    this.ack(result);
    this.tickContext.rootRouter().tell(addressedMessage, ActorRef.noSender());
  }

  @Override
  public void ack(final DataItem<?> item) {
    final int hash = this.tickContext.tickInfo().ackerRange().from();

    final AddressedMessage<?> addressedMessage = new AddressedMessage<>(new Ack(item.ack(), item.meta().globalTime()), hash, this.tickContext.tickInfo().startTs());
    this.tickContext.rootRouter().tell(addressedMessage, ActorRef.noSender());
  }

  @Override
  public Optional<Object> loadState(final InPort inPort) {
    final byte[] key = Longs.toByteArray(inPort.id());
    final byte[] value = db.get(key);
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
  public void saveState(final InPort inPort, final Object state) {
    final byte[] key = Longs.toByteArray(inPort.id());
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      final ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(state);
      oos.close();

      final byte[] value = bos.toByteArray();
      bos.close();
      db.put(key, value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeState(final InPort inPort) {
    final byte[] key = Longs.toByteArray(inPort.id());
    db.delete(key);
  }

  @Override
  public HashRange localRange() {
    return this.tickContext.localRange();
  }
}
