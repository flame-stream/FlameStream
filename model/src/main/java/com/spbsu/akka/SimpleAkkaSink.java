package com.spbsu.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import com.spbsu.commons.filters.Filter;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Experts League
 * Created by solar on 10.11.16.
 */
public class SimpleAkkaSink<T> {
  private static final String EOS_MARKER = "End of stream";
  private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
  private ActorRef actorRef;
  private final Class<T> clazz;
  private final Filter<Object> eosFilter;

  public SimpleAkkaSink(Class<T> clazz, Filter<Object> eosFilter) {
    this.clazz = clazz;
    this.eosFilter = eosFilter;
  }

  public Stream<T> stream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iter<T>(), Spliterator.IMMUTABLE), false);
  }

  public ActorRef actor(ActorSystem akka) {
    if (actorRef != null)
      return actorRef;
    return actorRef = akka.actorOf(ActorContainer.props(SinkActor.class, queue, eosFilter));
  }

  public class Iter<T> implements Iterator<T> {
    private T next;
    private boolean eos = false;

    @Override
    public boolean hasNext() {
      if (next != null)
        return true;
      if (eos)
        return false;
      try {

        final Object take = queue.take();
        if (!clazz.isAssignableFrom(take.getClass())) {
          if (EOS_MARKER.equals(take))
            eos = true;
          else
            return hasNext();
        }
        else
          //noinspection unchecked
          next = (T) take;
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      return !eos;
    }

    @Override
    public T next() {
      try {
        if (!hasNext())
          return null;
        return next;
      }
      finally {
        next = null;
      }
    }
  }

  public static class SinkActor extends ActorAdapter<UntypedActor> {
    private final LinkedBlockingQueue<Object> queue;
    private final Filter<Object> eosFilter;

    public SinkActor(LinkedBlockingQueue<Object> queue, Filter<Object> eosFilter) {
      this.queue = queue;
      this.eosFilter = eosFilter;
    }

    @ActorMethod
    public void append(Object di) {
      if(eosFilter.accept(di))
        context().stop(self());
      queue.add(di);
    }

    @Override
    protected void postStop() {
      queue.add(EOS_MARKER); // poison pill
      super.postStop();
    }
  }
}
