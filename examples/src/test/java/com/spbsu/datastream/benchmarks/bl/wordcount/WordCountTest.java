package com.spbsu.datastream.benchmarks.bl.wordcount;

import com.spbsu.datastream.benchmarks.bl.DataStreamsSource;
import com.spbsu.datastream.benchmarks.bl.FlinkSource;
import com.spbsu.datastream.benchmarks.bl.TestSource;
import com.spbsu.datastream.benchmarks.bl.wordcount.model.WordCounter;
import com.spbsu.datastream.core.LocalCluster;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * User: Artem
 * Date: 19.06.2017
 */
public class WordCountTest {
  private static final FlinkSource FLINK_INDEX_SOURCE = new FlinkSource<String>(0) {
    @Override
    public void test(Stream<String> input, Consumer<Object> output) {
      WordCountFlinkRunner.test(input, output, this.bufferTimeout);
    }
  };

  @DataProvider
  public Object[][] provider() {
    return new Object[][]{
            {dataStreamsSource(1, 4, 20)},
            {dataStreamsSource(4, 4, 20)},
            {FLINK_INDEX_SOURCE}
    };
  }

  @Test(dataProvider = "provider")
  public void test(TestSource testSource) {
    final Map<String, Integer> actual = new HashMap<>();
    final List<String> source = Stream.generate(() -> text(100)).limit(10).collect(Collectors.toList());
    //noinspection unchecked
    testSource.test(source.stream(), o -> {
      final WordCounter wordContainer = (WordCounter) o;
      actual.putIfAbsent(wordContainer.word(), 0);
      actual.computeIfPresent(wordContainer.word(), (uid, old) -> Math.max(wordContainer.count(), old));
    });
    final Map<String, Integer> expected = source.stream()
            .map(text -> text.split("\\s"))
            .flatMap(Arrays::stream)
            .collect(toMap(Function.identity(), o -> 1, Integer::sum));
    Assert.assertEquals(actual, expected);
  }

  @SuppressWarnings("SameParameterValue")
  private String text(int size) {
    final String[] words = {"repka", "dedka", "babka", "zhuchka", "vnuchka"};
    return new Random().ints(size, 0, words.length)
            .mapToObj(i -> words[i])
            .collect(joining(" "));
  }

  @SuppressWarnings("SameParameterValue")
  private DataStreamsSource<String> dataStreamsSource(int fronts, int workers, int tickLength) {
    return new DataStreamsSource<String>(fronts, workers, tickLength) {
      @Override
      public void test(Stream<String> input, Consumer<Object> output) {
        try {
          try (final LocalCluster cluster = new LocalCluster(this.workers, this.fronts)) {
            WordCountRunner.test(cluster, input, output, this.tickLength);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
