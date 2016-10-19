package experiments.interfaces.nikita.fake;

import experiments.interfaces.nikita.*;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by marnikitta on 19.10.16.
 */
public class InMemoryStream<S> implements YetAnotherStream<S> {
    private final Stream<Tuple2<Timestamp, S>> inner;

    private final Type<S> type;

    private final boolean isValid;

    public InMemoryStream(final Supplier<S> supplier, final Type<S> type, final long limit) {
        this(Stream.generate(supplier).limit(limit).map(i -> new Tuple2<>(Timestamp.now(), i)), type, true);
    }

    private InMemoryStream(final Stream<Tuple2<Timestamp, S>> stream, final Type<S> type, final boolean isValid) {
        this.inner = stream;
        this.type = type;
        this.isValid = isValid;
    }

    @Override
    public Type<S> type() {
        return this.type;
    }

    @Override
    public double correlationWithMeta(final Function<? super S, ? extends Comparable> comparable,
                                      final CorrelationType type) {
        return 0;
    }

    @Override
    public <T> YetAnotherStream<T> map(final Filter<S, T> filter) {
        boolean isValid = filter.isConsumed(type);
        return new InMemoryStream<T>(inner.map(t -> new Tuple2<>(t.v1(), filter.apply(t.v2()))), filter.a2b(type()), isValid);
    }

    @Override
    public boolean isValid() {
        List<RunningCondition<S>> conds = type.conditions().stream().map(Condition::instance).collect(Collectors.toList());
        inner.map(Tuple2::v2).forEach(s -> {
            conds.forEach(c -> c.update(s));
        });

        return Seq.seq(conds.stream()).foldLeft(true, (seed, condition) -> seed && condition.isValid()) && this.isValid;
    }
}
