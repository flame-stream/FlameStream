package com.spbsu.flamestream.example.wordcount;

import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.example.wordcount.model.WordCounter;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.util.AwaitConsumer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * User: Artem
 * Date: 19.12.2017
 */
public class WordCountTest extends FlameStreamSuite {
  @DataProvider
  public static Object[][] provider() {
    return new Object[][] {
            {false},
            {true}
    };
  }

  @Test(dataProvider = "provider")
  public void localEnvironmentTest(boolean backPressure) throws InterruptedException {
    final LocalRuntime runtime = new LocalRuntime(4, 100);
    final FlameRuntime.Flame flame = runtime.run(new WordCountGraph().get());
    {
      final int lineSize = 100;
      final Collection<String> input = Stream.generate(() -> {
        final String[] words = {"repka", "dedka", "babka", "zhuchka", "vnuchka"};
        return new Random().ints(lineSize, 0, words.length).mapToObj(i -> words[i]).collect(joining(" "));
      }).limit(1000).collect(Collectors.toList());

      final AwaitConsumer<WordCounter> awaitConsumer = new AwaitConsumer<>(lineSize * input.size());
      flame.attachRear("Rear", new AkkaRearType<>(runtime.system(), WordCounter.class))
              .forEach(r -> r.addListener(awaitConsumer));
      final Consumer<Object> sink = randomConsumer(
              flame.attachFront("Sink", new AkkaFrontType<>(runtime.system(), backPressure))
                      .collect(Collectors.toList())
      );
      input.forEach(sink);

      awaitConsumer.await(5, TimeUnit.MINUTES);
      final Map<String, Integer> actual = new HashMap<>();
      awaitConsumer.result().forEach(o -> {
        final WordCounter wordContainer = o;
        actual.putIfAbsent(wordContainer.word(), 0);
        actual.computeIfPresent(wordContainer.word(), (uid, old) -> Math.max(wordContainer.count(), old));
      });
      final Pattern pattern = Pattern.compile("\\s");
      final Map<String, Integer> expected = input.stream()
              .map(pattern::split)
              .flatMap(Arrays::stream)
              .collect(toMap(Function.identity(), o -> 1, Integer::sum));

      Assert.assertEquals(actual, expected);
    }
  }
}
