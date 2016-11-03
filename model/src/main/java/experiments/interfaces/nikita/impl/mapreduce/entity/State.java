package experiments.interfaces.nikita.impl.mapreduce.entity;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Created by marnikitta on 03.11.16.
 */
public class State {
    private final String query;

    private final long count;

    private State(final String query, final long count) {
        this.query = query;
        this.count = count;
    }

    public State(final String query) {
        this(query, 1);
    }

    public String query() {
        return query;
    }

    public long count() {
        return count;
    }

    public State incremented() {
        return new State(this.query, this.count + 1);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
                .append("query", query)
                .append("count", count)
                .toString();
    }
}
