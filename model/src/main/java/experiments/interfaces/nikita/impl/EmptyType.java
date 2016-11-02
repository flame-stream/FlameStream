package experiments.interfaces.nikita.impl;

import experiments.interfaces.nikita.Condition;
import experiments.interfaces.nikita.Type;

import java.util.Collections;
import java.util.Set;

/**
 * Created by marnikitta on 02.11.16.
 */
public class EmptyType<S> implements Type<S> {
    @Override
    public String name() {
        return "empty-type";
    }

    @Override
    public Set<Condition<S>> conditions() {
        return Collections.emptySet();
    }
}
