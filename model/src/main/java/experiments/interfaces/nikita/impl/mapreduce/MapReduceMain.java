package experiments.interfaces.nikita.impl.mapreduce;

import experiments.interfaces.nikita.YetAnotherStream;
import experiments.interfaces.nikita.impl.EmptyType;
import experiments.interfaces.nikita.impl.SimpleFilter;
import experiments.interfaces.nikita.impl.SimpleGrouping;
import experiments.interfaces.nikita.impl.mapreduce.entity.EntryType;
import experiments.interfaces.nikita.impl.mapreduce.entity.State;
import experiments.interfaces.nikita.impl.mapreduce.entity.StateOrUserQuery;
import experiments.interfaces.nikita.impl.mapreduce.entity.UserQuery;
import javaa.util.stream.YetAnotherStreamSupport;

import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by marnikitta on 03.11.16.
 */
public class MapReduceMain {
    public static void main(String[] args) {
        final Stream<String> queries = new Random().ints(0, 10).mapToObj(qId -> "Query#" + qId);
        final Stream<UserQuery> userQueries = queries.map(query -> new UserQuery(UUID.randomUUID().toString(), query)).limit((int) 1e2);

        final YetAnotherStream<UserQuery> mainStream = YetAnotherStreamSupport.stream(userQueries, new EmptyType<>());
        final YetAnotherStream<StateOrUserQuery> mainStreamToMerge = mainStream.filter((SimpleFilter<UserQuery, StateOrUserQuery>) StateOrUserQuery::new);

        final Promise<YetAnotherStream<StateOrUserQuery>> stateStreamToMerge = new Promise<>();

        final YetAnotherStream<StateOrUserQuery> merged = mainStreamToMerge.mergeWith(stateStreamToMerge::get);
        final YetAnotherStream<List<StateOrUserQuery>> grouped = merged.groupBy(
                (SimpleGrouping<StateOrUserQuery>) stateOrUserQuery -> stateOrUserQuery.query().hashCode(),
                2
        );

        final YetAnotherStream<State> rawStateStream = grouped.filter(new UserCountFilter());

        final YetAnotherStream<StateOrUserQuery> branch = rawStateStream.split().filter((SimpleFilter<State, StateOrUserQuery>) StateOrUserQuery::new);

        stateStreamToMerge.setValue(branch);

        final List<State> result = rawStateStream.collect(Collectors.toList());
        result.forEach(System.out::println);
    }

    static class UserCountFilter implements SimpleFilter<List<StateOrUserQuery>, State> {
        @Override
        public State apply(final List<StateOrUserQuery> stateOrUserQueries) {
            assert stateOrUserQueries.size() <= 2;

            if (stateOrUserQueries.size() == 1) {
                final UserQuery userQuery = stateOrUserQueries.get(0).userQuery().orElseThrow(IllegalStateException::new);
                return new State(userQuery.query());
            } else if (stateOrUserQueries.get(0).type() == EntryType.STATE) {
                final State state = stateOrUserQueries.get(0).state().orElseThrow(IllegalStateException::new);
                final UserQuery userQuery = stateOrUserQueries.get(1).userQuery().orElseThrow(IllegalStateException::new);
                return state.incremented();
            } else {
                return null;
            }
        }
    }

    static class Promise<T> {
        private T value;

        public void setValue(final T value) {
            this.value = value;
        }

        public T get() {
            return Objects.requireNonNull(value);
        }
    }

}
