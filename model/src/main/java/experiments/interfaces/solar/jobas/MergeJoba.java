package experiments.interfaces.solar.jobas;

import experiments.interfaces.solar.DataItem;
import experiments.interfaces.solar.DataType;
import experiments.interfaces.solar.Joba;
import experiments.interfaces.solar.SystemTime;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class MergeJoba extends Joba.Stub {
  private final List<Joba> merge = new ArrayList<>();

  public MergeJoba(DataType generates, Joba... merge) {
    super(generates);
    this.merge.addAll(Arrays.asList(merge));
  }

  public void add(Joba oneMore) {
    merge.add(oneMore);
  }

  @Override
  public Stream<DataItem> materialize(Stream<DataItem> seed) {
    //noinspection unchecked,SuspiciousToArrayCall
    final Spliterator<DataItem>[] mergeSpliterators = merge.stream().map(joba -> joba.materialize(seed)).collect(Collectors.toList()).toArray(new Spliterator[0]);
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<DataItem>() {
      final DataItem[] merge = new DataItem[mergeSpliterators.length];
      boolean initialized = false;
      DataItem next;
      @Override
      public boolean hasNext() {
        if (!initialized) {
          for (int i = 0; i < merge.length; i++) {
            int finalI = i;
            mergeSpliterators[i].tryAdvance(next -> merge[finalI] = next);
          }
          initialized = true;
        }

        int minTimeSource = -1;
        SystemTime minTime = SystemTime.INFINITY;
        for (int i = 0; i < merge.length; i++) {
          if (merge[i] != null && minTime.greater(merge[i].meta().time())) {
            minTimeSource = i;
            minTime = merge[i].meta().time();
          }
        }
        if (minTimeSource < 0)
          return false;
        next = merge[minTimeSource];
        int finalMinTimeSource = minTimeSource;
        if (!mergeSpliterators[minTimeSource].tryAdvance(next -> merge[finalMinTimeSource] = next))
          merge[minTimeSource] = null;
        return true;
      }

      @Override
      public DataItem next() {
        return next;
      }
    }, Spliterator.IMMUTABLE), false);
  }
}
