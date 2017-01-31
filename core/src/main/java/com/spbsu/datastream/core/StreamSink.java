package com.spbsu.datastream.core;
import com.spbsu.datastream.core.job.control.Control;
import com.spbsu.datastream.core.job.control.EndOfTick;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamSink implements Sink{
  private static final String EOS_MARKER = "End of stream";
  private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();

  public Stream<Object> stream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new StreamSink.Iter(), Spliterator.IMMUTABLE), false);
  }

  @Override
  public void accept(DataItem item) {
    queue.add(item);
  }

  @Override
  public void accept(Control control) {
    queue.add(control);
    if(control instanceof EndOfTick) {
      queue.add(EOS_MARKER);
    }
  }

  private class Iter implements Iterator<Object> {
    private Object next;
    private boolean eos = false;

    @Override
    public boolean hasNext() {
      if (next != null)
        return true;
      if (eos)
        return false;
      try {

        final Object take = queue.take();
        if (!DataItem.class.isAssignableFrom(take.getClass()) && !Control.class.isAssignableFrom(take.getClass())) {
          if (EOS_MARKER.equals(take))
            eos = true;
          else
            return hasNext();
        }
        else
          //noinspection unchecked
          next = take;
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      return !eos;
    }

    @Override
    public Object next() {
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

}
