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

  public Stream<DataItem> stream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new StreamSink.Iter(), Spliterator.IMMUTABLE), false);
  }

  @Override
  public void accept(DataItem item) {
    queue.add(item);
  }

  @Override
  public void accept(Control control) {
    if(control instanceof EndOfTick) {
      queue.add(EOS_MARKER);
    }
  }

  public class Iter implements Iterator<DataItem> {
    private DataItem next;
    private boolean eos = false;

    @Override
    public boolean hasNext() {
      if (next != null)
        return true;
      if (eos)
        return false;
      try {

        final Object take = queue.take();
        if (!DataItem.class.isAssignableFrom(take.getClass())) {
          if (EOS_MARKER.equals(take))
            eos = true;
          else
            return hasNext();
        }
        else
          //noinspection unchecked
          next = (DataItem) take;
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      return !eos;
    }

    @Override
    public DataItem next() {
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
