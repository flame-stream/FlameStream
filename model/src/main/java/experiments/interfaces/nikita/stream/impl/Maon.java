package experiments.interfaces.nikita.stream.impl;

import experiments.interfaces.nikita.stream.EmptyType;
import experiments.interfaces.nikita.stream.YetAnotherStream;

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
        YetAnotherStream<Integer> stream1 = stream.split();
        YetAnotherStream<Integer> stream2 = stream.mergeWith(stream1);

        stream2.materialize().forEach(System.out::println);
    }
}
