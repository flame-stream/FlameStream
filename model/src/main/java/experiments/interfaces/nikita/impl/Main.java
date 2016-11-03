package experiments.interfaces.nikita.impl;

import experiments.interfaces.nikita.YetAnotherStream;
import javaa.util.stream.YetAnotherStreamSupport;

import java.util.stream.IntStream;

/**
 * Created by marnikitta on 19.10.16.
 */
public class Main {
    public static void main(String[] args) {
        final YetAnotherStream<Integer> sp = YetAnotherStreamSupport.stream(IntStream.range(0, 10).boxed(), new EmptyType<>());

        sp.groupBy((SimpleGrouping<Integer>) integer -> integer % 2, 2).forEach(System.out::println);
    }

    private static class Counter {
        volatile long cnt = 0;

        void increment() {
            cnt++;
        }
    }
}
