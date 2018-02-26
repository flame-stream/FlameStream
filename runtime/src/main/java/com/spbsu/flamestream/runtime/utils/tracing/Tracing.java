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
  private static final int DEFAULT_SIZE = 10_000;
  private static final int DEFAULT_SAMPLING = 101;
  private static final Tracer EMPTY_TRACER = new EmptyTracer();
  private static final boolean TRACING_ENABLED = Boolean.parseBoolean(System.getProperty("enable.tracing", "false"));

  public static final Tracing TRACING = new Tracing();
  private final Map<String, Tracer> tracers = Collections.synchronizedMap(new HashMap<>());

  public Tracer forEvent(String event) {
    if (TRACING_ENABLED) {
      tracers.putIfAbsent(event, new TracerImpl(event, DEFAULT_SIZE, DEFAULT_SAMPLING));
    }
    return tracers.getOrDefault(event, EMPTY_TRACER);
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

  public interface Tracer {
    void log(long id);

    void log(long id, long time);

    void flush(PrintWriter pw);
  }

  private static class TracerImpl implements Tracer {
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

    @Override
    public void flush(PrintWriter pw) {
      final int total = Math.min(offset.get(), size);
      for (int i = 0; i < total; ++i) {
        pw.println(event + ',' + id[i] + ',' + (ts[i]));
      }
    }
  }

  private static final class EmptyTracer implements Tracer {
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
}
