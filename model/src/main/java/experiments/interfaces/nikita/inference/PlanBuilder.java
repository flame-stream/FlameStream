package experiments.interfaces.nikita.inference;

import experiments.interfaces.nikita.stream.Type;

/**
 * Created by marnikitta on 04.11.16.
 */
public interface PlanBuilder {
    <S, R> Plan<S, R> filterChain(Type<S> from, Type<R> to);
}
