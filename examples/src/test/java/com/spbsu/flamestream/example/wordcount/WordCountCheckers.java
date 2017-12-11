package com.spbsu.flamestream.example.wordcount;

import com.spbsu.flamestream.example.ExampleChecker;
import com.spbsu.flamestream.example.wordcount.model.WordCounter;
import org.testng.Assert;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public enum WordCountCheckers implements ExampleChecker<String, WordCounter> {
  CHECK_COUNT {
    private final List<String> input = Stream.generate(() -> text(1000))
            .limit(10).collect(Collectors.toList());

    @Override
    public Stream<String> input() {
      return input.stream();
    }

    @Override
    public void assertCorrect(Stream<WordCounter> output) {
      final Map<String, Integer> actual = new HashMap<>();
      output.forEach(o -> {
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

    private String text(int size) {
      final String[] words = {"repka", "dedka", "babka", "zhuchka", "vnuchka"};
      return new Random().ints(size, 0, words.length).mapToObj(i -> words[i]).collect(joining(" "));
    }
  }
}
