package experiments.interfaces.nikita;

import org.jooq.lambda.tuple.Tuple4;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Created by marnikitta on 19.10.16.
 */
public interface Filter<S1, S2>
        extends Function<S1, S2> {

    boolean isConsumed(Type<? extends S1> type);

    boolean isProduced(Type<? extends S2> type);

    // Shitty naming
    Type<S2> a2b(Type<? extends S1> from);

    // Shitty naming too
    Type<S1> b2a(Type<? extends S2> to);

    // Weight is not part of Filter
}
