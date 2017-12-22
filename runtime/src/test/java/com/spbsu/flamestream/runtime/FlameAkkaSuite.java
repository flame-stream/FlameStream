package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 22.12.2017
 */
public class FlameAkkaSuite extends FlameStreamSuite {
  protected <T> void applyDataToHandles(List<Stream<T>> data, List<AkkaFrontType.Handle<T>> handles) {
    if (data.size() != handles.size()) {
      throw new IllegalArgumentException("data.size() != handles.size()");
    }
    IntStream.range(0, handles.size()).forEach(i -> {
      final Thread thread = new Thread(() -> {
        final AkkaFrontType.Handle<T> handle = handles.get(i);
        data.get(i).forEach(handle);
        handle.eos();
      });
      thread.setDaemon(true);
      thread.start();
    });
  }
}
