package experiments.interfaces.nikita.inference;

import experiments.interfaces.nikita.stream.Filter;
import experiments.interfaces.nikita.stream.Type;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by marnikitta on 05.11.16.
 */
public class Repository {
    private final Set<Type> types = new HashSet<>();

    private final Set<Filter> filters = new HashSet<>();

    public void registerType(final Type<?> type) {
        types.add(type);
    }

    public void registerFilter(final Filter<?, ?> filter) {
        filters.add(filter);
    }

    public Set<Type> types() {
        return Collections.unmodifiableSet(types);
    }

    public Set<Filter> filters() {
        return Collections.unmodifiableSet(filters);
    }
}
