package experiments.interfaces.nikita.impl.mapreduce.entity;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.stream.Stream;

/**
 * Created by marnikitta on 03.11.16.
 */
public class UserQuery {
    private final String user;

    private final String query;

    public UserQuery(final String user, final String query) {
        this.user = user;
        this.query = query;
    }

    public String user() {
        return user;
    }

    public String query() {
        return query;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
                .append("user", user)
                .append("query", query)
                .toString();
    }
}
