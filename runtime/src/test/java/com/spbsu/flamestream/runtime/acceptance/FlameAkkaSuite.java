package com.spbsu.flamestream.runtime.acceptance;

import com.expleague.commons.text.charset.TextDecoder;
import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 22.12.2017
 */
public class FlameAkkaSuite extends FlameStreamSuite {
  protected static int DEFAULT_PARALLELISM = 4;
  private final Object lock = new Object();

  protected <T> void applyDataToHandleAsync(Stream<T> data, AkkaFront.FrontHandle<T> handle) {
    final Thread thread = new Thread(() -> {
      data.forEach(handle);
      handle.eos();
    });
    thread.setDaemon(true);
    thread.start();
  }


  protected <T> void applyDataToAllHandlesAsync(Queue<T> data, List<AkkaFront.FrontHandle<T>> handles) {
    IntStream.range(0, handles.size()).forEach(i -> {
      final Thread thread = new Thread(() -> {
        final AkkaFront.FrontHandle<T> handle = handles.get(i);
        while (true) {
          final T item = data.poll();
          if (item != null) {
            handle.accept(item);
          } else {
            handle.eos();
            break;
          }
        }
      });
      thread.setDaemon(true);
      thread.start();
    });
  }


  protected <T> void applyDataToAllHandlesAsync(Stream<T> data, List<AkkaFront.FrontHandle<T>> handles) {
    final Spliterator<T> spliterator = data.spliterator();
    IntStream.range(0, handles.size()).forEach(i -> {
      final Thread thread = new Thread(() -> {
        final AkkaFront.FrontHandle<T> handle = handles.get(i);
        while (true) {
          AtomicReference<T> sss = new AtomicReference<T>(null);
          synchronized (spliterator) {
            boolean success = spliterator.tryAdvance(t -> {
              //handle.accept(t);
              sss.set(t);
            });
            if (!success) {
              break;
            }
          }
          handle.accept(sss.get());
        }
        System.out.format("end for handle %d %s%n", i, Thread.currentThread().getId());
        handle.eos();
      });
      thread.setDaemon(true);
      thread.start();
    });
  }
}
