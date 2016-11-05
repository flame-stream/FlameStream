package experiments.interfaces.solar.jobas;

import experiments.interfaces.solar.DataItem;
import experiments.interfaces.solar.DataType;
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
  private final Joba base;
  private List<DataItem> buffer = new ArrayList<>(1000);
  private TIntArrayList positions = new TIntArrayList();
  private int start = 0;
  private Spliterator<DataItem> baseSpliterator;
  private boolean eos = false;

  public ReplicatorJoba(Joba base) {
    super(base.generates());
    this.base = base;
  }

  @Override
  public synchronized Stream<DataItem> materialize(final Stream<DataItem> seed) {
    if (baseSpliterator == null)
      baseSpliterator = base.materialize(seed).spliterator();

    final int index = positions.size();
    positions.add(0);
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
    positions.set(index, positions.get(index) + 1);
    if (positions.min() > start + maximumWaste) { // advance buffer
      final ArrayList<DataItem> nextBuffer = new ArrayList<>(Math.max(1000, buffer.size() - 100));
      for (int i = maximumWaste; i < buffer.size(); i++) {
        nextBuffer.add(buffer.get(i));
      }
      buffer = nextBuffer;
      start += maximumWaste;
    }
    return next - start < buffer.size() || (!eos && baseSpliterator.tryAdvance(buffer::add));
  }
}
