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
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * User: Artem
 * Date: 04.01.2018
 */
public class IndexFunction extends RichMapFunction<Tuple2<String, long[]>, Result> {
  private transient ValueState<InvertedIndexState> checkpointedState;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.checkpointedState = getRuntimeContext().getState(new ValueStateDescriptor<>(
            "index_state",
            new GenericTypeInfo<>(InvertedIndexState.class)
    ));
    new Thread(() -> {
      while (true) {
        try {
          Runtime.getRuntime()
                  .exec("find /opt/flink/data -mindepth 2 -type d -cmin +0.2 -exec rm -rf {} +")
                  .waitFor();
          TimeUnit.SECONDS.sleep(10);
        } catch (IOException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }).start();
  }

  @Override
  public Result map(Tuple2<String, long[]> value) throws IOException {
    InvertedIndexState sta = checkpointedState.value();
    if (sta == null) {
      sta = new InvertedIndexState();
    }

    final long prevValue = sta.updateOrInsert(value.f1);
    final WordIndexAdd wordIndexAdd = new WordIndexAdd(value.f0, value.f1);
    WordIndexRemove wordIndexRemove = null;
    if (prevValue != InvertedIndexState.PREV_VALUE_NOT_FOUND) {
      wordIndexRemove = new WordIndexRemove(
              value.f0,
              IndexItemInLong.setRange(prevValue, 0),
              IndexItemInLong.range(prevValue)
      );
    }
    checkpointedState.clear();
    checkpointedState.update(sta);
    return new Result(wordIndexAdd, wordIndexRemove);
  }
}
