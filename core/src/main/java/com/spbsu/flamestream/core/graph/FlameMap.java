package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.Meta;
import org.jetbrains.annotations.Nullable;

import java.util.stream.Stream;

public class FlameMap<T, R> extends HashingVertexStub {
  private final SerializableFunction<T, Stream<R>> function;
  private final Class<?> clazz;
  private final @Nullable
  HashFunction hashFunction;
  private final SerializableConsumer<HashGroup> init;

  public FlameMap(
          SerializableFunction<T, Stream<R>> function,
          Class<?> clazz,
          @Nullable HashFunction hashFunction,
          SerializableConsumer<HashGroup> init
  ) {
    this.function = function;
    this.clazz = clazz;
    this.hashFunction = hashFunction;
    this.init = init;
  }

  public FlameMap(SerializableFunction<T, Stream<R>> function, Class<?> clazz, SerializableConsumer<HashGroup> init) {
    this(function, clazz, null, init);
  }

  public FlameMap(SerializableFunction<T, Stream<R>> function, Class<?> clazz, @Nullable HashFunction hashFunction) {
    this(function, clazz, hashFunction, __ -> {});
  }

  public FlameMap(SerializableFunction<T, Stream<R>> function, Class<?> clazz) {
    this(function, clazz, null, __ -> {});
  }

  public FlameMapOperation operation(long physicalId) {
    return new FlameMapOperation(physicalId);
  }

  @Override
  public String toString() {
    return "FlameMap{" +
            "function=" + function +
            '}';
  }

  public SerializableFunction<T, Stream<R>> function() {
    return function;
  }

  public void init(HashGroup hashGroup) {
    init.accept(hashGroup);
  }

  @Override
  public @Nullable
  HashFunction hash() {
    return hashFunction;
  }

  public class FlameMapOperation {
    private final long physicalId;

    FlameMapOperation(long physicalId) {
      this.physicalId = physicalId;
    }

    public Stream<DataItem> apply(DataItem dataItem) {
      //noinspection unchecked
      final Stream<R> result = function.apply(dataItem.payload((Class<T>) clazz));
      final int[] childId = {0};
      return result.map(r -> {
        final Meta newMeta = new Meta(dataItem.meta(), physicalId, childId[0]++);
        return new PayloadDataItem(newMeta, r);
      });
    }
  }
}
