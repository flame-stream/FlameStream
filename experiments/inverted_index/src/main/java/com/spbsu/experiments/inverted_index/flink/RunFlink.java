package com.spbsu.experiments.inverted_index.flink;

import com.spbsu.experiments.inverted_index.common_bl.actions.PageToWordPositionsFilter;
import com.spbsu.experiments.inverted_index.common_bl.io.WikiPageIterator;
import com.spbsu.experiments.inverted_index.common_bl.models.WikiPage;
import com.spbsu.experiments.inverted_index.common_bl.models.WordContainer;
import com.spbsu.experiments.inverted_index.common_bl.models.WordPagePosition;
import com.spbsu.experiments.inverted_index.common_bl.utils.InvertedIndexStorage;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 05.03.2017
 * Time: 12:03
 */
public class RunFlink {
  private static Iterator<WikiPage> iterator;
  private static long counter = 0;

  public static void main(String[] args) throws Exception {
    final ClassLoader classLoader = RunFlink.class.getClassLoader();
    final URL fileUrl = classLoader.getResource("wikipedia/small_dump_example.xml");
    if (fileUrl == null) {
      throw new RuntimeException("Dump URL is null");
    }
    final File dumpFile = new File(fileUrl.getFile());
    final InputStream inputStream = new FileInputStream(dumpFile);

    iterator = new WikiPageIterator(inputStream);
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
    env.setBufferTimeout(0);

    final DataStream<WikiPage> stream = env.addSource(new WikiPagesSource());
    //noinspection Convert2Lambda
    stream.flatMap(new Splitter())
            .keyBy(0)
            .fold(new InvertedIndexStorage(), new FoldFunction<Tuple2<String, long[]>, InvertedIndexStorage>() {
              @Override
              public InvertedIndexStorage fold(InvertedIndexStorage accumulator, Tuple2<String, long[]> value) throws Exception {
                System.out.println(counter++);
                accumulator.updateOrInsert(value.f1);
                return accumulator;
              }
            });
    env.execute();
  }

  public static class WikiPagesSource implements SourceFunction<WikiPage> {
    private boolean running = true;

    @Override
    public void run(SourceContext<WikiPage> ctx) throws Exception {
      while (running) {
        if (iterator.hasNext()) {
          ctx.collect(iterator.next());
        } else {
          running = false;
          ctx.close();
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  public static class Splitter implements FlatMapFunction<WikiPage, Tuple2<String, long[]>> {
    @Override
    public void flatMap(WikiPage value, Collector<Tuple2<String, long[]>> out) throws Exception {
      final PageToWordPositionsFilter filter = new PageToWordPositionsFilter();
      final Stream<WordContainer> result = filter.apply(value);
      result.forEach(v -> {
        final WordPagePosition wpp = (WordPagePosition) v;
        out.collect(new Tuple2<>(wpp.word(), wpp.positions()));
      });
    }
  }
}
