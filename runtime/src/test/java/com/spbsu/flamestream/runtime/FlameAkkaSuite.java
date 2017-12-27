package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;

import java.util.List;
import java.util.Queue;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 22.12.2017
 */
public class FlameAkkaSuite extends FlameStreamSuite {
  protected static int DEFAULT_PARALLELISM = 4;

  protected <T> void applyDataToHandleAsync(Stream<T> data, AkkaFrontType.Handle<T> handle) {
    final Thread thread = new Thread(() -> {
      data.forEach(handle);
      handle.eos();
    });
    thread.setDaemon(true);
    thread.start();
  }

  protected <T> void applyDataToAllHandlesAsync(Queue<T> data, List<AkkaFrontType.Handle<T>> handles) {
    IntStream.range(0, handles.size()).forEach(i -> {
      final Thread thread = new Thread(() -> {
        final AkkaFrontType.Handle<T> handle = handles.get(i);
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
}
