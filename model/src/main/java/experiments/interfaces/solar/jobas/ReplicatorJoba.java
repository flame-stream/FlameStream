package experiments.interfaces.solar.jobas;

import experiments.interfaces.solar.DataItem;
import experiments.interfaces.solar.Joba;
import gnu.trove.list.array.TIntArrayList;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class ReplicatorJoba extends Joba.Stub {
  private final static int maximumWaste = 100;
  private List<DataItem> buffer = new ArrayList<>(1000);
  private TIntArrayList positions = new TIntArrayList();
  private int start = 0;

  private Spliterator<DataItem> baseSpliterator;
  private boolean eos = false;
  private final Thread producer;
  private Stream<DataItem> seed;

  public ReplicatorJoba(final Joba base) {
    super(base.generates());
    producer = new Thread(() -> {
      baseSpliterator = base.materialize(seed).spliterator();
      while(true) {
        if (!baseSpliterator.tryAdvance((e) -> {
          synchronized (ReplicatorJoba.this) {
            buffer.add(e);
          }
        }))
          break;
        synchronized (ReplicatorJoba.this) {
          ReplicatorJoba.this.notifyAll();
        }
      }
      synchronized (ReplicatorJoba.this) {
        eos = true;
        ReplicatorJoba.this.notifyAll();
      }
    });

  }

  private boolean started = false;
  @Override
  public synchronized Stream<DataItem> materialize(final Stream<DataItem> seed) {
    if (!started) {
      started = true;
      this.seed = seed;
      producer.start();
    }
    final int index = positions.size();
    positions.add(-1);
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<DataItem>() {
      @Override
      public boolean hasNext() {
        return incrementIndex(index);
      }
      @Override
      public DataItem next() {
        return elementFor(index);
      }
    }, Spliterator.IMMUTABLE), false);
  }

  private synchronized DataItem elementFor(int index) {
    return buffer.get(positions.get(index) - start);
  }

  private synchronized boolean incrementIndex(int index) {
    final int next = positions.get(index) + 1;
    positions.set(index, next);
//    if (positions.min() > start + maximumWaste) { // advance buffer
//      final ArrayList<DataItem> nextBuffer = new ArrayList<>(Math.max(1000, buffer.size() - 100));
//      for (int i = maximumWaste; i < buffer.size(); i++) {
//        nextBuffer.add(buffer.get(i));
//      }
//      buffer = nextBuffer;
//      start += maximumWaste;
//    }
    while (!eos && next - start >= buffer.size()) {
      try {
        wait();
      }
      catch (InterruptedException ignore) {}
    }
    if (!eos || next - start < buffer.size())
      return true;
    while (positions.min() - start < buffer.size()) {
      try {
        wait();
      }
      catch (InterruptedException ignore) {}
    }
    return false;
  }
}
