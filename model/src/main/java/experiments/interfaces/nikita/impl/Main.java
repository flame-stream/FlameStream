package experiments.interfaces.nikita.impl;

import experiments.interfaces.nikita.DataItem;
import experiments.interfaces.nikita.YetAnotherStream;
import javaa.util.stream.InMemoryStreamSupport;

import java.util.Spliterator;
import java.util.stream.IntStream;

/**
 * Created by marnikitta on 19.10.16.
 */
public class Main {
    public static void main(String[] args) {
        Spliterator<DataItem<String>> sp = IntStream.range(0, 100).boxed().map(i -> new SimpleDataItem<>(Integer.toString(i), new FineGrainedMeta(i)))
                .map(di -> (DataItem<String>) di).spliterator();
        YetAnotherStream<String> stream1 = InMemoryStreamSupport.stream(sp, new EmptyType<>());
        YetAnotherStream<String> stream2 = stream1.trySplit();
        YetAnotherStream<String> stream3 = stream1.trySplit();
        stream1.forEach(System.out::print);
        System.out.println();
        stream2.forEach(System.out::print);
        System.out.println();
        stream3.forEach(System.out::print);
    }
}
