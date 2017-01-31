package com.spbsu.datastream.core.sink;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.job.control.Control;
import com.spbsu.datastream.core.job.control.EndOfTick;

import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * Created by marnikitta on 1/31/17.
 */
public class SinkSpliterator extends Spliterators.AbstractSpliterator<Object> implements Sink {
  private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
  private boolean isCompleted = false;

  public SinkSpliterator() {
    super(Long.MAX_VALUE, 0);
  }

  @Override
  public void accept(DataItem item) {
    assertIsNotCompleted();

    queue.add(item);
  }

  @Override
  public void accept(Control control) {
    assertIsNotCompleted();

    if (control instanceof EndOfTick) {
      isCompleted = true;
    }
    queue.add(control);
  }

  private void assertIsNotCompleted() {
    if (isCompleted) {
      throw new IllegalStateException("End of tick already appeared");
    }
  }

  @Override
  public boolean tryAdvance(Consumer<? super Object> action) {
    if (isCompleted) {
      return false;
    } else {
      try {
        action.accept(queue.take());
        return true;
      } catch (InterruptedException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }
}
