package experiments.interfaces.nikita.inference;

import experiments.interfaces.nikita.stream.YetAnotherStream;

import java.util.function.Function;

/**
 * Created by marnikitta on 04.11.16.
 */
public interface Plan<S, R> extends Function<YetAnotherStream<S>, YetAnotherStream<R>>{
}
