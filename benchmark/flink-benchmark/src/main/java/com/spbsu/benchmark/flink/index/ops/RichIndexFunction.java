package com.spbsu.benchmark.flink.index.ops;

import com.spbsu.benchmark.flink.index.Result;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.model.WordIndexRemove;
import com.spbsu.flamestream.example.bl.index.ops.InvertedIndexState;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.concurrent.atomic.AtomicInteger;

public class RichIndexFunction extends RichMapFunction<Tuple2<String, long[]>, Result> {
  private transient ValueState<InvertedIndexState> state = null;
  private transient AtomicInteger prevDocId;

  @Override
  public void open(Configuration parameters) {
    final ValueStateDescriptor<InvertedIndexState> descriptor = new ValueStateDescriptor<>(
            "index",
            InvertedIndexState.class
    );

    state = getRuntimeContext().getState(descriptor);
    prevDocId = new AtomicInteger(-1);
  }

  @Override
  public Result map(Tuple2<String, long[]> value) throws Exception {
    final int docId = IndexItemInLong.pageId(value.f1[0]);
    if (prevDocId.getAndSet(docId) > docId) {
      throw new IllegalStateException("Output doc ids are not monotonic");
    }

    final InvertedIndexState currentStat;
    if (state.value() == null) {
      currentStat = new InvertedIndexState();
    } else {
      currentStat = state.value();
    }

    final long prevValue = currentStat.updateOrInsert(value.f1);
    final WordIndexAdd wordIndexAdd = new WordIndexAdd(value.f0, value.f1);
    WordIndexRemove wordIndexRemove = null;
    if (prevValue != InvertedIndexState.PREV_VALUE_NOT_FOUND) {
      wordIndexRemove = new WordIndexRemove(
              value.f0,
              IndexItemInLong.setRange(prevValue, 0),
              IndexItemInLong.range(prevValue)
      );
    }
    state.update(currentStat);
    return new Result(wordIndexAdd, wordIndexRemove);
  }
}
