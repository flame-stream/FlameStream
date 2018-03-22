package com.spbsu.benchmark.flink.index.ops;

import com.spbsu.benchmark.flink.index.Result;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.model.WordIndexRemove;
import com.spbsu.flamestream.example.bl.index.ops.InvertedIndexState;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * User: Artem
 * Date: 04.01.2018
 */
public class IndexFunction extends ProcessFunction<Tuple2<String, long[]>, Result> implements CheckpointedFunction {
  private transient ValueState<InvertedIndexState> checkpointedState;
  private transient InvertedIndexState index;

  @Override
  public void processElement(Tuple2<String, long[]> value, Context ctx, Collector<Result> out) {
    final long prevValue = index.updateOrInsert(value.f1);

    final WordIndexAdd wordIndexAdd = new WordIndexAdd(value.f0, value.f1);
    WordIndexRemove wordIndexRemove = null;
    if (prevValue != InvertedIndexState.PREV_VALUE_NOT_FOUND) {
      wordIndexRemove = new WordIndexRemove(
              value.f0,
              IndexItemInLong.setRange(prevValue, 0),
              IndexItemInLong.range(prevValue)
      );
    }
    out.collect(new Result(wordIndexAdd, wordIndexRemove));
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    checkpointedState.clear();
    checkpointedState.update(index);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    this.checkpointedState = context.getKeyedStateStore().getState(new ValueStateDescriptor<>(
            "index_state",
            new GenericTypeInfo<>(InvertedIndexState.class)
    ));

    index = new InvertedIndexState();
  }
}
