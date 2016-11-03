package experiments.interfaces.nikita.impl.mapreduce.entity;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;
import java.util.Optional;

/**
 * Created by marnikitta on 03.11.16.
 */
public class StateOrUserQuery {
    private final UserQuery userQuery;

    private final State state;

    private final EntryType type;

    public StateOrUserQuery(final UserQuery userQuery) {
        this.type = EntryType.USER_QUERY;
        this.userQuery = Objects.requireNonNull(userQuery);
        this.state = null;
    }

    public StateOrUserQuery(final State state) {
        this.type = EntryType.STATE;
        this.state = Objects.requireNonNull(state);
        this.userQuery = null;
    }

    public Optional<UserQuery> userQuery() {
        return Optional.ofNullable(userQuery);
    }

    public Optional<State> state() {
        return Optional.ofNullable(state);
    }

    public String query() {
        if (this.type == EntryType.USER_QUERY) {
            return this.userQuery.query();
        } else {
            return this.state.query();
        }
    }

    public EntryType type() {
        return type;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this,ToStringStyle.NO_CLASS_NAME_STYLE)
                .append("userQuery", userQuery)
                .append("state", state)
                .append("type", type)
                .toString();
    }
}
