package experiments.interfaces.nikita.stream.impl;

import experiments.interfaces.nikita.stream.impl.spliterator.*;
import experiments.interfaces.nikita.stream.impl.util.MetaGenerator;
import experiments.interfaces.nikita.stream.*;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by marnikitta on 04.11.16.
 */
public class YetAnotherInMemoryStream<S> implements YetAnotherStream<S> {
    private Stream<DataItem<S>> baseStream;

    private final Type<S> type;

    private final Set<RunningCondition<S>> conditions;

    private final Consumer<DataItem<S>> conditionConsumer;

    public YetAnotherInMemoryStream(final Stream<S> baseStream, final Type<S> type) {
        final Supplier<FineGrainedMeta> metaSupplier = new MetaGenerator();
        this.type = type;
        this.conditions = runningConditions(this.type);
        this.conditionConsumer = conditionConsumers(this.conditions);
        this.baseStream = baseStream.map(s -> new DataItem<>(s, metaSupplier.get())).peek(conditionConsumer);
    }

    private YetAnotherInMemoryStream(final Spliterator<DataItem<S>> baseSpliterator, final Type<S> type) {
        this.type = type;
        this.conditions = runningConditions(this.type);
        this.conditionConsumer = conditionConsumers(this.conditions);
        this.baseStream = StreamSupport.stream(baseSpliterator, false).peek(conditionConsumer);
    }

    private Set<RunningCondition<S>> runningConditions(final Type<S> type) {
        return type.conditions().stream().map(Condition::instance).collect(Collectors.toSet());
    }

    private Consumer<DataItem<S>> conditionConsumers(final Collection<RunningCondition<S>> conditions) {
        return (item) -> conditions.forEach(cond -> cond.update(item.value()));
    }

    @Override
    public Type<S> type() {
        return this.type;
    }

    @Override
    public boolean isValid() {
        return conditions.stream().allMatch(RunningCondition::isValid);
    }

    @Override
    public Stream<S> materialize() {
        return baseStream.map(DataItem::value);
    }

    @Override
    public YetAnotherStream<S> split() {
        final SpliteratorReplicator<DataItem<S>> replicator = new SpliteratorReplicator<>(baseStream.spliterator());
        this.baseStream = StreamSupport.stream(replicator.split(), false);
        return new YetAnotherInMemoryStream<>(replicator.split(), type());
    }

    @Override
    public <R> YetAnotherStream<R> map(final Function<? super S, ? extends R> mapper, final Type<R> targetType) {
        final Spliterator<DataItem<R>> spliterator = baseStream
                .map(dataItem -> new DataItem<>((R) mapper.apply(dataItem.value()), dataItem.meta()))
                .filter(dataItem -> dataItem.value() != null)
                .spliterator();
        return new YetAnotherInMemoryStream<>(spliterator, targetType);
    }

    @Override
    public YetAnotherStream<S> mergeWith(final YetAnotherStream<S> that) {
        if (!(that instanceof YetAnotherInMemoryStream)) {
            throw new IllegalArgumentException("That and this YAStreams should have identical implementation");
        }
        final YetAnotherInMemoryStream<S> thatOk = (YetAnotherInMemoryStream<S>) that;

        final MergingSpliterator<DataItem<S>> mergingSpliterator = new MergingSpliterator<>(baseStream.spliterator(), thatOk.baseStream.spliterator());
        return new YetAnotherInMemoryStream<S>(mergingSpliterator, this.type);
    }

    @Override
    public YetAnotherStream<S> mergeWith(final Supplier<YetAnotherStream<S>> that) {
        final Supplier<Spliterator<DataItem<S>>> promise = () -> {
            final YetAnotherStream<S> realStream = that.get();
            if (!(realStream instanceof YetAnotherInMemoryStream)) {
                throw new IllegalArgumentException("That and this YAStreams should have identical implementation");
            }
            final YetAnotherInMemoryStream<S> thatOk = (YetAnotherInMemoryStream<S>) realStream;
            return thatOk.baseStream.spliterator();
        };
        final Spliterator<DataItem<S>> lazy = new LazySpliterator<>(promise);

        final MergingSpliterator<DataItem<S>> mergingSpliterator = new MergingSpliterator<>(baseStream.spliterator(), lazy);
        return new YetAnotherInMemoryStream<S>(mergingSpliterator, this.type);
    }

    @Override
    public YetAnotherStream<List<S>> groupBy(final Grouping<S> grouping, final int window) {
        final Grouping<DataItem<S>> dataItemGrouping = new Grouping<DataItem<S>>() {
            @Override
            public boolean equals(final DataItem<S> s1, final DataItem<S> s2) {
                return grouping.equals(s1.value(), s2.value());
            }

            @Override
            public int applyAsInt(final DataItem<S> value) {
                return grouping.applyAsInt(value.value());
            }
        };

        final Spliterator<List<DataItem<S>>> spliterator = new GroupingSpliterator<>(this.baseStream.spliterator(), dataItemGrouping, window);
        final Spliterator<DataItem<List<S>>> resilt = new MappingSpliterator<>(spliterator, list -> {
            final List<S> valueList = list.stream().map(DataItem::value).collect(Collectors.toList());
            final Meta meta = list.stream().map(DataItem::meta).max(Comparator.comparing((Meta m) -> (FineGrainedMeta) m)).orElse(new FineGrainedMeta(0)).incremented();
            return new DataItem<>(valueList, meta);
        });
        return new YetAnotherInMemoryStream<>(resilt, new EmptyType<>());
    }
}
