package com.spbsu.benchmark.flink;

import com.spbsu.flamestream.example.wordcount.model.WordCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * User: Artem
 * Date: 05.10.2017
 */
public class WordCountStream implements FlinkStream<String, WordCounter> {

  @Override
  public DataStream<WordCounter> stream(DataStream<String> source) {
    //noinspection deprecation
    return source.flatMap(new Splitter()).keyBy(0)
      //fold is deprecated but there is no alternative in the current version
      .fold(new WordCounter(null, 0), new WordCounterFold());
  }

  private static class Splitter implements FlatMapFunction<String, Tuple1<String>> {
    @Override
    public void flatMap(String value, Collector<Tuple1<String>> out) throws Exception {
      Arrays.stream(value.split("\\s")).forEach(s -> out.collect(new Tuple1<>(s)));
    }
  }

  @SuppressWarnings("deprecation")
  private static class WordCounterFold implements FoldFunction<Tuple1<String>, WordCounter> {
    @Override
    public WordCounter fold(WordCounter accumulator, Tuple1<String> value) throws Exception {
      return new WordCounter(value.f0, accumulator.count() + 1);
    }
  }
}
