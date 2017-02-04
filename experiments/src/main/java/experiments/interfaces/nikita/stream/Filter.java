package experiments.interfaces.nikita.stream;

import java.util.function.Function;

/**
 * Created by marnikitta on 19.10.16.
 */
public interface Filter<S1, S2>
        extends Function<YetAnotherStream<S1>, YetAnotherStream<S2>> {

  boolean accepts(Type<S1> type);

  boolean generates(Type<S2> type);

  Type<S2> input2output(Type<S1> from);

  Type<S1> output2input(Type<S2> to);
}
