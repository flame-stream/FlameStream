package javaa.util.stream;

import experiments.interfaces.nikita.*;
import experiments.interfaces.nikita.impl.EmptyType;
import javaa.util.MergingSpliterator;
import javaa.util.concurrent.QueueSpliterator;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.*;
import java.util.stream.Collector;

/**
 * Created by marnikitta on 02.11.16.
 */
abstract class YetAnotherPipeline<E_IN, E_OUT>
        extends AbstractPipeline<DataItem<E_IN>, DataItem<E_OUT>, YetAnotherStream<E_OUT>>
        implements YetAnotherStream<E_OUT> {

    private final Type<E_OUT> type;

    private final Set<RunningCondition<E_OUT>> conditions;

    @SuppressWarnings("WeakerAccess")
    protected Consumer<DataItem<E_OUT>> pipeConsumer = (item) -> {
    };

    private YetAnotherPipeline(final Supplier<? extends Spliterator<?>> source, final int sourceFlags, final boolean parallel, final Type<E_OUT> type) {
        super(source, sourceFlags, parallel);
        this.type = type;
        this.conditions = runningConditions(type);
        this.pipeConsumer = conditionConsumers(this.conditions);
    }

    private YetAnotherPipeline(final Spliterator<?> source, final int sourceFlags, final boolean parallel, final Type<E_OUT> type) {
        super(source, sourceFlags, parallel);
        this.type = type;
        this.conditions = runningConditions(type);
        this.pipeConsumer = conditionConsumers(this.conditions);
    }

    private YetAnotherPipeline(final AbstractPipeline<?, DataItem<E_IN>, ?> previousStage, final int opFlags, final Type<E_OUT> type) {
        super(previousStage, opFlags);
        this.type = type;
        this.conditions = runningConditions(type);
        this.pipeConsumer = conditionConsumers(this.conditions);
    }

    private Set<RunningCondition<E_OUT>> runningConditions(final Type<E_OUT> type) {
        return type.conditions().stream().map(Condition::instance).collect(java.util.stream.Collectors.toSet());
    }

    private Consumer<DataItem<E_OUT>> conditionConsumers(final Collection<RunningCondition<E_OUT>> conditions) {
        Consumer<DataItem<E_OUT>> result = (cons) -> {
        };

        for (RunningCondition<E_OUT> condition : conditions) {
            result = result.andThen(di -> condition.update(di.value()));
        }

        return result;
    }

    @Override
    public Type<E_OUT> type() {
        return this.type;
    }

    @Override
    public <R> YetAnotherStream<R> filter(final Filter<E_OUT, R> filter) {
        Objects.requireNonNull(filter);

        if (!filter.isConsumed(this.type)) {
            throw new IllegalArgumentException("Filter should accept type " + this.type);
        }

        return new StatelessOp<E_OUT, R>(this, StreamOpFlag.NOT_SORTED | StreamOpFlag.NOT_DISTINCT, filter.a2b(this.type)) {
            @Override
            Sink<DataItem<E_OUT>> opWrapSink(final int flags, final Sink<DataItem<R>> sink) {
                return new Sink.ChainedReference<DataItem<E_OUT>, DataItem<R>>(sink) {
                    @Override
                    public void accept(final DataItem<E_OUT> u) {
                        final DataItem<R> result = u.map(filter).incremented();

                        if (result.value() != null) {
                            pipeConsumer.accept(result);
                            downstream.accept(result);
                        }
                    }
                };
            }
        };
    }

    @Override
    public YetAnotherStream<E_OUT> mergeWith(final YetAnotherStream<E_OUT> that) {
        final Spliterator<DataItem<E_OUT>> spliterator = new MergingSpliterator<>(
                this.spliterator(),
                that.spliterator(),
                Comparator.comparing(Function.identity())
        );

        return new Head<>(spliterator, StreamOpFlag.fromCharacteristics(spliterator.characteristics()), false, type);
    }

    @Override
    public YetAnotherStream<E_OUT> mergeWith(final Supplier<YetAnotherStream<E_OUT>> that) {
        Supplier<Spliterator<DataItem<E_OUT>>> spliteratorSupplier = () -> that.get().spliterator();
        final Spliterator<DataItem<E_OUT>> spliterator = new MergingSpliterator<>(
                this.spliterator(),
                lazySpliterator(spliteratorSupplier),
                Comparator.comparing(Function.identity())
        );

        return new Head<>(spliterator, StreamOpFlag.fromCharacteristics(spliterator.characteristics()), false, type);
    }

    @Override
    public YetAnotherStream<E_OUT> split() {
        final BlockingQueue<DataItem<E_OUT>> queue = new LinkedBlockingQueue<>();
        final Spliterator<DataItem<E_OUT>> spliterator = new QueueSpliterator<>(queue, 10);

        pipeConsumer = pipeConsumer.andThen(queue::add);

        return new Head<>(spliterator, StreamOpFlag.fromCharacteristics(spliterator.characteristics()), false, type);
    }

    @Override
    public YetAnotherStream<List<E_OUT>> groupBy(final Grouping<E_OUT> grouping, final int window) {
        return new YetAnotherPipeline.StatefulOp<E_OUT, List<E_OUT>>(this, StreamOpFlag.NOT_SIZED, new EmptyType<>()) {

            @Override
            Sink<DataItem<E_OUT>> opWrapSink(final int flags, final Sink<DataItem<List<E_OUT>>> sink) {
                Objects.requireNonNull(sink);

                return new Sink.ChainedReference<DataItem<E_OUT>, DataItem<List<E_OUT>>>(sink) {
                    private final Map<Integer, List<E_OUT>> groupingState = new HashMap<>();

                    @Override
                    public void begin(final long size) {
                        downstream.begin(-1);
                    }

                    @Override
                    public void end() {
                        groupingState.clear();
                        downstream.end();
                    }

                    @Override
                    public void accept(final DataItem<E_OUT> t) {
                        final int hash = grouping.apply(t.value());

                        final List<E_OUT> hashList = groupingState.getOrDefault(grouping.apply(t.value()), new ArrayList<>());
                        hashList.add(t.value());
                        final List<E_OUT> result = hashList.subList(Math.max(hashList.size() - window, 0), hashList.size());

                        groupingState.put(hash, result);

                        downstream.accept(new DataItem<>(result, t.meta().incremented()));
                    }
                };
            }

            @Override
            <P_IN> Node<DataItem<List<E_OUT>>> opEvaluateParallel(
                    final PipelineHelper<DataItem<List<E_OUT>>> helper, final Spliterator<P_IN> spliterator,
                    final IntFunction<DataItem<List<E_OUT>>[]> generator) {
                throw new IllegalArgumentException();
            }
        };
    }

    @Override
    public boolean isValid() {
        return conditions.stream().anyMatch(RunningCondition::isValid);
    }

    @Override
    public <R, A> R collect(final Collector<? super E_OUT, A, R> collector) {
        A container;
        container = collector.supplier().get();
        BiConsumer<A, ? super E_OUT> accumulator = collector.accumulator();
        forEach(u -> accumulator.accept(container, u));

        return collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)
                ? (R) container
                : collector.finisher().apply(container);
    }

    @Override
    public void forEach(final Consumer<? super E_OUT> consumer) {
        evaluate(ForEachOps.makeRef(value -> consumer.accept(value.value()), false));
    }

    @Override
    public YetAnotherStream<E_OUT> peek(final Consumer<? super E_OUT> action) {
        Objects.requireNonNull(action);
        return new StatelessOp<E_OUT, E_OUT>(this, 0, type) {
            @Override
            Sink<DataItem<E_OUT>> opWrapSink(final int flags, final Sink<DataItem<E_OUT>> sink) {
                return new Sink.ChainedReference<DataItem<E_OUT>, DataItem<E_OUT>>(sink) {
                    @Override
                    public void accept(final DataItem<E_OUT> u) {
                        pipeConsumer.accept(u);
                        action.accept(u.value());
                        downstream.accept(u);
                    }
                };
            }
        };
    }

    @Override
    boolean opIsStateful() {
        return false;
    }

    @Override
    Sink<DataItem<E_IN>> opWrapSink(final int flags, final Sink<DataItem<E_OUT>> sink) {
        return null;
    }

    @Override
    StreamShape getOutputShape() {
        return StreamShape.REFERENCE;
    }

    @Override
    <P_IN> Node<DataItem<E_OUT>> evaluateToNode(final PipelineHelper<DataItem<E_OUT>> helper, final Spliterator<P_IN> spliterator, final boolean flattenTree, final IntFunction<DataItem<E_OUT>[]> generator) {
        return Nodes.collect(helper, spliterator, flattenTree, generator);
    }

    @Override
    <P_IN> Spliterator<DataItem<E_OUT>> wrap(final PipelineHelper<DataItem<E_OUT>> ph, final Supplier<Spliterator<P_IN>> supplier, final boolean isParallel) {
        return new StreamSpliterators.WrappingSpliterator<>(ph, supplier, isParallel);
    }

    @Override
    Spliterator<DataItem<E_OUT>> lazySpliterator(final Supplier<? extends Spliterator<DataItem<E_OUT>>> supplier) {
        return new StreamSpliterators.DelegatingSpliterator<>(supplier);
    }

    @Override
    void forEachWithCancel(final Spliterator<DataItem<E_OUT>> spliterator, final Sink<DataItem<E_OUT>> sink) {
        do {
        } while (!sink.cancellationRequested() && spliterator.tryAdvance(sink));
    }

    @Override
    Node.Builder<DataItem<E_OUT>> makeNodeBuilder(final long exactSizeIfKnown, final IntFunction<DataItem<E_OUT>[]> generator) {
        return Nodes.builder(exactSizeIfKnown, generator);
    }

    @Override
    public Iterator<DataItem<E_OUT>> iterator() {
        return Spliterators.iterator(spliterator());
    }

    @Override
    public YetAnotherStream<E_OUT> unordered() {
        if (!isOrdered())
            return this;
        return new StatelessOp<E_OUT, E_OUT>(this, StreamOpFlag.NOT_ORDERED, this.type) {
            @Override
            Sink<DataItem<E_OUT>> opWrapSink(int flags, Sink<DataItem<E_OUT>> sink) {
                return sink;
            }
        };
    }

    static class Head<E_IN, E_OUT> extends YetAnotherPipeline<E_IN, E_OUT> {

        Head(final Supplier<? extends Spliterator<?>> source,
             final int sourceFlags, final boolean parallel,
             final Type<E_OUT> type) {
            super(source, sourceFlags, parallel, type);
        }

        Head(final Spliterator<?> source,
             final int sourceFlags, final boolean parallel,
             final Type<E_OUT> type) {
            super(source, sourceFlags, parallel, type);
        }

        @Override
        final boolean opIsStateful() {
            throw new UnsupportedOperationException();
        }

        @Override
        final Sink<DataItem<E_IN>> opWrapSink(int flags, Sink<DataItem<E_OUT>> sink) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forEach(final Consumer<? super E_OUT> action) {
            if (!isParallel()) {
                sourceStageSpliterator().forEachRemaining(dataItem -> {
                    pipeConsumer.accept(dataItem);
                    action.accept(dataItem.value());
                });
            } else {
                super.forEach(action);
            }
        }
    }

    abstract static class StatelessOp<E_IN, E_OUT>
            extends YetAnotherPipeline<E_IN, E_OUT> {

        StatelessOp(final AbstractPipeline<?, DataItem<E_IN>, ?> upstream,
                    final int opFlags,
                    final Type<E_OUT> type) {
            super(upstream, opFlags, type);
        }

        @Override
        final boolean opIsStateful() {
            return false;
        }
    }

    abstract static class StatefulOp<E_IN, E_OUT>
            extends YetAnotherPipeline<E_IN, E_OUT> {

        StatefulOp(final AbstractPipeline<?, DataItem<E_IN>, ?> upstream,
                   final int opFlags,
                   final Type<E_OUT> type) {
            super(upstream, opFlags, type);
        }

        @Override
        final boolean opIsStateful() {
            return true;
        }

        @Override
        abstract <P_IN> Node<DataItem<E_OUT>> opEvaluateParallel(final PipelineHelper<DataItem<E_OUT>> helper, final Spliterator<P_IN> spliterator, final IntFunction<DataItem<E_OUT>[]> generator);
    }
}
