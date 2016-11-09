package experiments.interfaces.solar.jobas;

import experiments.interfaces.solar.DataItem;
import experiments.interfaces.solar.DataType;
import experiments.interfaces.solar.Joba;
import experiments.interfaces.solar.SystemTime;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class MergeJoba extends Joba.Stub {
  private final List<Joba> merge = new ArrayList<>();
  private final int synthetics;

  public MergeJoba(DataType generates, int backLinks, Joba... merge) {
    super(generates);
    this.synthetics = backLinks;
    this.merge.addAll(Arrays.asList(merge));
  }

  public void add(Joba oneMore) {
    merge.add(oneMore);
  }

  private enum State {
    READY,
    INIT,
    WAIT,
    EOS
  }

  @Override
  public Stream<DataItem> materialize(Stream<DataItem> seed) {
    final int mergeCount = merge.size();
    //noinspection unchecked,SuspiciousToArrayCall
    final DataItem[] mergeData = new DataItem[mergeCount];
    final State[] states = new State[mergeCount]; Arrays.fill(states, State.INIT);
    final ForkJoinPool consumers = new ForkJoinPool(mergeCount);
    //noinspection unchecked
    final Spliterator<DataItem>[] mergeSpliterators = merge.stream().map(joba -> joba.materialize(seed)).map(BaseStream::spliterator).collect(Collectors.toList()).toArray(new Spliterator[0]);
    for (int i = 0; i < mergeCount; i++) {
      final int finalI = i;
      consumers.execute(() -> {
        while(true) {
          synchronized (MergeJoba.this) {
            while (states[finalI] == State.READY) {
              try {
                MergeJoba.this.wait();
              }
              catch (InterruptedException ignore) {
              }
            }
            states[finalI] = State.WAIT;
            MergeJoba.this.notifyAll();
          }
          if (!mergeSpliterators[finalI].tryAdvance(di -> mergeData[finalI] = di)) {
            synchronized (MergeJoba.this) {
              states[finalI] = State.EOS;
              mergeData[finalI] = null;
              MergeJoba.this.notifyAll();
              break;
            }
          }
          synchronized (MergeJoba.this) {
            states[finalI] = State.READY;
            MergeJoba.this.notifyAll();
          }
        }
      });
    }

    final Iterator<DataItem> iterator = new Iterator<DataItem>() {
      DataItem next;

      @Override
      public boolean hasNext() {
        int waiting = 0;
        int minTimeSource;
        do {
          minTimeSource = -1;
          SystemTime minTime = SystemTime.INFINITY;
          for (int i = 0; i < mergeCount; i++) {
            synchronized (MergeJoba.this) {
              while (states[i] == State.INIT) {
                try {
                  MergeJoba.this.wait();
                }
                catch (InterruptedException ignore) {}
              }
            }

            if (states[i] == State.EOS)
              continue;
            if (states[i] == State.WAIT)
              Thread.yield();
            if (states[i] == State.WAIT) {
              waiting++;
              continue;
            }
            if (minTime.greater(mergeData[i].meta().time())) {
              minTimeSource = i;
              minTime = mergeData[i].meta().time();
            }
          }

          if (minTimeSource >= 0)
            break;
          if (waiting > 0) {
            synchronized (MergeJoba.this) {
              try {
                MergeJoba.this.wait();
              }
              catch (InterruptedException ignore) { }
            }
          }
          else break;
        }
        while (true);
        if (minTimeSource < 0)
          return false;
        next = mergeData[minTimeSource];
        states[minTimeSource] = State.INIT;
        synchronized (MergeJoba.this) {
          MergeJoba.this.notifyAll();
        }
        return true;
      }

      @Override
      public DataItem next() {
        return next;
      }
    };
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.IMMUTABLE), false);
  }
}
