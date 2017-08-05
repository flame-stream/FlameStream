package com.spbsu.datastream.core.user_count;

import akka.actor.ActorPath;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.TestStand;
import com.spbsu.datastream.core.barrier.PreSinkMetaFilter;
import com.spbsu.datastream.core.barrier.RemoteActorConsumer;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.graph.ops.Broadcast;
import com.spbsu.datastream.core.graph.ops.Filter;
import com.spbsu.datastream.core.graph.ops.Grouping;
import com.spbsu.datastream.core.graph.ops.Merge;
import com.spbsu.datastream.core.graph.ops.StatelessMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 19.06.2017
 */
public class UserCountTest {
  private static final HashFunction<UserContainer> USER_HASH = new HashFunction<UserContainer>() {
    @Override
    public int hash(UserContainer value) {
      return value.user().hashCode();
    }
  };

  private static final BiPredicate<UserContainer, UserContainer> EQUALZ = new BiPredicate<UserContainer, UserContainer>() {
    @Override
    public boolean test(UserContainer o1, UserContainer o2) {
      return o1.user().equals(o2.user());
    }
  };

  private static final HashFunction<List<UserContainer>> GROUP_HASH = new HashFunction<List<UserContainer>>() {
    @Override
    public int hash(List<UserContainer> value) {
      return USER_HASH.hash(value.get(0));
    }
  };

  @Test
  public void testSingleFront() throws InterruptedException {
    this.test(1);
  }

  @Test
  public void testMultipleFronts() throws InterruptedException {
    this.test(4);
  }

  private void test(int fronts) throws InterruptedException {
    try (TestStand stage = new TestStand(4, fronts)) {
      final Map<String, Integer> actual = new HashMap<>();
      stage.deploy(userCountTest(stage.fronts(), stage.wrap(o -> {
        final UserCounter userCounter = (UserCounter) o;
        actual.putIfAbsent(userCounter.user(), 0);
        actual.computeIfPresent(userCounter.user(), (uid, old) -> Math.max(userCounter.count(), old));
      })), 30, TimeUnit.SECONDS);

      final String[] users = new String[]{"vasya", "petya", "kolya", "natasha"};
      final List<UserQuery> source = Stream.generate(() -> new UserQuery(users[ThreadLocalRandom.current().nextInt(0, users.length)]))
              .limit(1000)
              .collect(Collectors.toList());
      final Map<String, Integer> expected = source.stream().collect(Collectors.toMap(UserQuery::user, o -> 1, Integer::sum));

      final Consumer<Object> sink = stage.randomFrontConsumer(123);
      source.forEach(sink);
      stage.waitTick(35, TimeUnit.SECONDS);

      Assert.assertEquals(actual, expected);
    }
  }

  private static TheGraph userCountTest(Collection<Integer> fronts, ActorPath consumer) {
    final Merge<UserContainer> merge = new Merge<>(Arrays.asList(USER_HASH, USER_HASH));
    final Grouping<UserContainer> grouping = new Grouping<>(USER_HASH, EQUALZ, 2);
    final Filter<List<UserContainer>> filter = new Filter<>(new WrongOrderingFilter(), GROUP_HASH);
    final StatelessMap<List<UserContainer>, UserCounter> counter = new StatelessMap<>(new CountUserEntries(), GROUP_HASH);
    final Broadcast<UserCounter> broadcast = new Broadcast<>(USER_HASH, 2);

    final PreSinkMetaFilter<UserCounter> metaFilter = new PreSinkMetaFilter<>(USER_HASH);
    final RemoteActorConsumer<UserCounter> sink = new RemoteActorConsumer<>(consumer);

    final Graph graph = merge.fuse(grouping, merge.outPort(), grouping.inPort())
            .fuse(filter, grouping.outPort(), filter.inPort())
            .fuse(counter, filter.outPort(), counter.inPort())
            .fuse(broadcast, counter.outPort(), broadcast.inPort())
            .fuse(metaFilter, broadcast.outPorts().get(0), metaFilter.inPort())
            .fuse(sink, metaFilter.outPort(), sink.inPort())
            .wire(broadcast.outPorts().get(1), merge.inPorts().get(1));

    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(Collectors.toMap(Function.identity(), e -> merge.inPorts().get(0)));
    return new TheGraph(graph, frontBindings);
  }
}
