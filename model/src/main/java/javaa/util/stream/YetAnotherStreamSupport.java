package javaa.util.stream;

import experiments.interfaces.nikita.DataItem;
import experiments.interfaces.nikita.Type;
import experiments.interfaces.nikita.YetAnotherStream;
import experiments.interfaces.nikita.impl.FineGrainedMeta;

import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Created by marnikitta on 02.11.16.
 */
public class YetAnotherStreamSupport {
    public static <S> YetAnotherStream<S> stream(final Spliterator<DataItem<S>> spliterator, final Type<S> type) {
        Objects.requireNonNull(spliterator);
        return new YetAnotherPipeline.Head<>(spliterator,
                StreamOpFlag.fromCharacteristics(spliterator),
                false, type
        );
    }

    public static <S> YetAnotherStream<S> stream(final Supplier<Spliterator<DataItem<S>>> spliterator, final Type<S> type) {
        Objects.requireNonNull(spliterator);
        return new YetAnotherPipeline.Head<>(spliterator, Spliterator.ORDERED, false, type);
    }

    public static <S> YetAnotherStream<S> stream(final Stream<S> stream, final Type<S> type) {
        Objects.requireNonNull(stream);

        Counter counter = new Counter(0);
        Spliterator<DataItem<S>> spliterator = stream.map(item -> new DataItem<>(item, new FineGrainedMeta(counter.counter++))).spliterator();

        return new YetAnotherPipeline.Head<>(spliterator,
                StreamOpFlag.fromCharacteristics(spliterator),
                false, type
        );
    }

    static class Counter {
        public volatile long counter;

        public Counter(final long counter) {
            this.counter = counter;
        }
    }
}
