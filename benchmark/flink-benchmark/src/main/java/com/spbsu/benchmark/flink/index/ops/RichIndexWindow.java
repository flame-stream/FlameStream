package com.spbsu.benchmark.flink.index.ops;

import com.spbsu.benchmark.flink.index.Result;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.model.WordIndexRemove;
import com.spbsu.flamestream.example.bl.index.ops.InvertedIndexState;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.jooq.lambda.Unchecked;

/**
 * User: Artem
 * Date: 04.01.2018
 */
public class RichIndexWindow extends RichWindowFunction<Tuple2<String, long[]>, Result, Tuple, TimeWindow> {
  private transient ValueState<InvertedIndexState> state = null;
  //private transient AtomicInteger prevDocId;

  @Override
  public void open(Configuration parameters) throws Exception {
    final ValueStateDescriptor<InvertedIndexState> descriptor = new ValueStateDescriptor<>(
            "index",
            InvertedIndexState.class
    );

    state = getRuntimeContext().getState(descriptor);
    //prevDocId = new AtomicInteger(-1);
  }

  @Override
  public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, long[]>> input, Collector<Result> out) throws Exception {
    input.forEach(Unchecked.consumer(value -> {
      /*final int docId = IndexItemInLong.pageId(value.f1[0]);
      if (prevDocId.getAndSet(docId) > docId) {
        throw new IllegalStateException("Output doc ids are not monotonic");
      }*/

      final InvertedIndexState currentStat;
      if (state.value() == null) {
        currentStat = new InvertedIndexState();
      } else {
        currentStat = state.value().copy();
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
      out.collect(new Result(wordIndexAdd, wordIndexRemove));
    }));
  }
}
