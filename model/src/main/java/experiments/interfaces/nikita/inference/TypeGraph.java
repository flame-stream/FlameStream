package experiments.interfaces.nikita.inference;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import experiments.interfaces.nikita.stream.Filter;
import experiments.interfaces.nikita.stream.Type;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.*;

/**
 * Created by marnikitta on 05.11.16.
 */
public class TypeGraph {
    private Set<Type> nodes = new HashSet<>();

    private Set<Edge> edges = new HashSet<>();

    private Multimap<Type, Edge> outgoing = HashMultimap.create();

    public <S, R> void addEdge(final Type<S> from, final Type<R> to, final Filter<? super S, ? extends R> filter) {
        final Edge<S, R> edge = new Edge<>(from, to, filter);
        nodes.add(from);
        nodes.add(to);
        edges.add(edge);
        outgoing.put(from, edge);
    }

    public <S, R> List<Filter> findPath(final Type<S> from, final Type<R> to) {
        if (!nodes.contains(from) || !nodes.contains(to)) {
            throw new NoSuchElementException();
        }

        final Set<Type> visited = new HashSet<>();
        final Queue<Type> bfs = new ArrayDeque<>();
        bfs.offer(from);

        while (!bfs.isEmpty()) {
            final Type current = bfs.poll();
            visited.add(current);

            for (Edge e : outgoing.get(current)) {
                if (!visited.contains(e.to())) {
                    bfs.offer(e.to());
                }
            }
        }
        return Collections.emptyList();
    }


    private static class Edge<S, R> {
        private final Filter<? super S, ? extends R> filter;

        private final Type<S> from;

        private final Type<R> to;

        private Edge(final Type<S> from, final Type<R> to, final Filter<? super S, ? extends R> filter) {
            this.filter = filter;
            this.from = from;
            this.to = to;
        }

        public Filter<? super S, ? extends R> filter() {
            return filter;
        }

        public Type<S> from() {
            return from;
        }

        public Type<R> to() {
            return to;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            Edge<?, ?> edge = (Edge<?, ?>) o;

            return new EqualsBuilder()
                    .append(filter, edge.filter)
                    .append(from, edge.from)
                    .append(to, edge.to)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(filter)
                    .append(from)
                    .append(to)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("filter", filter)
                    .append("from", from)
                    .append("to", to)
                    .toString();
        }
    }
}
