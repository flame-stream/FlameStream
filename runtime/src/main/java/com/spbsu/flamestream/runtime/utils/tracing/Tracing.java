package com.spbsu.flamestream.runtime.utils.tracing;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Tracing {
  private static final int SIZE = 100_000;
  private static final int SAMPLING = 101;

  public static final Tracing TRACING = new Tracing();
  private final Map<String, Tracer> tracers = Collections.synchronizedMap(new HashMap<>());

  public Tracer forEvent(String event) {
    tracers.putIfAbsent(event, new TracerImpl(event, SIZE, SAMPLING));
    return tracers.get(event);
  }

  public Tracer forEvent(String event, int expectedSize, int sampling) {
    tracers.putIfAbsent(event, new TracerImpl(event, expectedSize, sampling));
    return tracers.get(event);
  }

  public void flush(Path path) throws IOException {
    try (final PrintWriter bw = new PrintWriter(Files.newBufferedWriter(
            path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
    ))) {
      tracers.values().forEach(t -> t.flush(bw));
    }
  }

  public final class TracerImpl implements Tracer {
    private final String event;
    private final int size;
    private final int sampling;

    private final long[] ts;
    private final long[] id;
    private final AtomicInteger offset = new AtomicInteger();
    private final AtomicInteger attempts = new AtomicInteger(0);

    public TracerImpl(String event, int size, int sampling) {
      this.ts = new long[size];
      this.id = new long[size];
      this.sampling = sampling;
      this.event = event;
      this.size = size;
    }

    @Override
    public void log(long id) {
      attempts.incrementAndGet();
      if (id % sampling == 0) {
        final int pos = offset.getAndIncrement();
        this.id[pos % size] = id;
        ts[pos % size] = System.nanoTime();
      }
    }

    @Override
    public void log(long id, long time) {
      attempts.incrementAndGet();
      if (id % sampling == 0) {
        final int pos = offset.getAndIncrement();
        this.id[pos % size] = id;
        ts[pos % size] = time;
      }
    }

    public void flush(PrintWriter pw) {
      System.out.println("Attempts of event '" + event + "' - " + attempts.get());
      final int total = Math.min(offset.get(), size);
      for (int i = 0; i < total; ++i) {
        pw.println(event + ',' + id[i] + ',' + (ts[i]));
      }
    }
  }

  public final class EmptyTracer implements Tracer {
    EmptyTracer(String event, int expectedSize, int sampling) {

    }

    @Override
    public void log(long id) {

    }

    @Override
    public void log(long id, long time) {

    }

    @Override
    public void flush(PrintWriter pw) {

    }
  }

  public interface Tracer {
    void log(long id);

    void log(long id, long time);

    void flush(PrintWriter pw);
  }

}
