package experiments.interfaces.nikita.impl;

import experiments.interfaces.nikita.DataItem;
import experiments.interfaces.nikita.YetAnotherStream;
import javaa.util.stream.YetAnotherStreamSupport;

import java.util.Spliterator;
import java.util.stream.IntStream;

/**
 * Created by marnikitta on 19.10.16.
 */
public class Main {
    public static void main(String[] args) {
        Spliterator<DataItem<String>> sp = IntStream.range(0, 10000000).boxed().map(i -> new SimpleDataItem<>(Integer.toString(i), new FineGrainedMeta(i)))
                .map(di -> (DataItem<String>) di).spliterator();
        YetAnotherStream<String> stream1 = YetAnotherStreamSupport.stream(sp, new EmptyType<>())
                .filter(SimpleFilter.identity());

        YetAnotherStream<String> stream2 = stream1.split().filter((SimpleFilter<String, String>) s -> "Second: " + s)
                .filter(SimpleFilter.identity()).filter(SimpleFilter.identity());
        stream1 = stream1.filter((SimpleFilter<String, String>) s -> "First: " + s);

        Counter counter = new Counter();

        stream1.mergeWith(stream2).forEach((s) -> counter.increment());

        System.out.println(counter.cnt);
    }

    private static class Counter {
        volatile long cnt = 0;

        void increment() {
            cnt++;
        }
    }
}
