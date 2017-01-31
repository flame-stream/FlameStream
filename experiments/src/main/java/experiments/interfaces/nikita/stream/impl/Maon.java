package experiments.interfaces.nikita.stream.impl;

import experiments.interfaces.nikita.stream.EmptyType;
import experiments.interfaces.nikita.stream.YetAnotherStream;
import experiments.interfaces.nikita.stream.impl.util.Promise;

import java.util.stream.IntStream;

/**
 * Created by marnikitta on 04.11.16.
 */
public class Maon {
  public static void main(String[] args) {
    YetAnotherStream<Integer> stream = new YetAnotherInMemoryStream<>(
            IntStream.range(0, 10).boxed(),
            new EmptyType<>()
    );
    Promise<YetAnotherStream<Integer>> promise = new Promise<>();

    YetAnotherStream<Integer> stream1 = stream
            .mergeWith(promise::get)
            .map(i -> i * 2, new EmptyType<>());

    promise.set(stream1.split());

    stream1.materialize().forEach(System.out::println);
  }
}
