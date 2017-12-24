package com.spbsu.flamestream.example.wordcount;

import com.spbsu.flamestream.example.wordcount.model.WordCounter;
import com.spbsu.flamestream.runtime.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitConsumer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
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
public class WordCountTest extends FlameAkkaSuite {
  @Test
  public void localEnvironmentTest() throws InterruptedException {
    final int parallelism = 4;
    try (final LocalRuntime runtime = new LocalRuntime(parallelism, 50)) {
      final FlameRuntime.Flame flame = runtime.run(new WordCountGraph().get());
      {
        final int lineSize = 50;
        final int streamSize = 500;
        final List<List<String>> input = Stream.generate(() -> Stream.generate(() -> {
          final String[] words = {"repka", "dedka", "babka", "zhuchka", "vnuchka"};
          return new Random().ints(lineSize, 0, words.length).mapToObj(i -> words[i])
                  .collect(joining(" "));
        }).limit(streamSize).collect(Collectors.toList())).limit(parallelism).collect(Collectors.toList());

        final AwaitConsumer<WordCounter> awaitConsumer = new AwaitConsumer<>(
                lineSize * streamSize * parallelism
        );
        flame.attachRear("Rear", new AkkaRearType<>(runtime.system(), WordCounter.class))
                .forEach(r -> r.addListener(awaitConsumer));
        final List<AkkaFrontType.Handle<String>> handles = flame
                .attachFront("Sink", new AkkaFrontType<String>(runtime.system(), true))
                .collect(Collectors.toList());
        applyDataToHandles(input.stream().map(Collection::stream).collect(Collectors.toList()), handles);
        awaitConsumer.await(5, TimeUnit.MINUTES);

        final Map<String, Integer> actual = new HashMap<>();
        awaitConsumer.result().forEach(o -> {
          final WordCounter wordContainer = o;
          actual.putIfAbsent(wordContainer.word(), 0);
          actual.computeIfPresent(wordContainer.word(), (uid, old) -> Math.max(wordContainer.count(), old));
        });
        final Pattern pattern = Pattern.compile("\\s");
        final Map<String, Integer> expected = input.stream()
                .flatMap(Collection::stream)
                .map(pattern::split)
                .flatMap(Arrays::stream)
                .collect(toMap(Function.identity(), o -> 1, Integer::sum));

        Assert.assertEquals(actual, expected);
      }
    }
  }
}
