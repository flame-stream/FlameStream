package com.spbsu.flamestream.example.nexmark;

import com.github.nexmark.flink.model.Auction;
import com.github.nexmark.flink.model.Event;
import com.github.nexmark.flink.model.Person;
import com.google.common.hash.Hashing;
import com.spbsu.flamestream.core.graph.SerializableFunction;
import com.spbsu.flamestream.example.labels.Flow;
import com.spbsu.flamestream.example.labels.Operator;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import scala.util.Either;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Query8 {
  public static final class PersonGroupingKey {
    public final long id;
    public final String name;
    public final long startTime;

    private PersonGroupingKey(long id, String name, long startTime) {
      this.id = id;
      this.name = name;
      this.startTime = startTime;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, startTime);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof PersonGroupingKey) {
        final var that = (PersonGroupingKey) obj;
        return Objects.equals(id, that.id) && Objects.equals(name, that.name) && startTime == that.startTime;
      }
      return false;
    }

    @Override
    public String toString() {
      return "(" + id + ", " + name + ", " + startTime + ")";
    }
  }

  private static final class AuctionGroupingKey {
    public final long seller;
    public final long startTime;

    private AuctionGroupingKey(long seller, long startTime) {
      this.seller = seller;
      this.startTime = startTime;
    }

    @Override
    public int hashCode() {
      return Objects.hash(seller, startTime);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof AuctionGroupingKey) {
        final var that = (AuctionGroupingKey) obj;
        return seller == that.seller && startTime == that.startTime;
      }
      return false;
    }

    @Override
    public String toString() {
      return "(" + seller + ", " + startTime + ")";
    }
  }

  private static final class HashedId {
    final long id;

    private HashedId(long id) {this.id = id;}

    @Override
    public int hashCode() {
      return Hashing.murmur3_32().hashLong(id).asInt();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof HashedId) {
        return id == ((HashedId) obj).id;
      }
      return false;
    }

    @Override
    public String toString() {
      return "" + id;
    }
  }

  public static Flow<Event, ?> create(long interval) {
    final var inputs = new Operator.Input<>(Event.class);
    return new Flow<>(inputs, selectJoinOn(
            selectUniqueInWindow(
                    PersonGroupingKey.class,
                    person -> new PersonGroupingKey(person.id, person.name, tumbleStart(person.dateTime, interval)),
                    inputs.flatMap(Person.class, event -> Stream.ofNullable(event.newPerson))
            ),
            selectUniqueInWindow(
                    AuctionGroupingKey.class,
                    auction -> new AuctionGroupingKey(auction.seller, tumbleStart(auction.dateTime, interval)),
                    inputs.flatMap(Auction.class, event -> Stream.ofNullable(event.newAuction))
            ),
            p -> new HashedId(p.id),
            a -> new HashedId(a.seller)
    ));
  }

  private static <Input, Output> Operator<Output> selectUniqueInWindow(
          Class<Output> outputClass,
          SerializableFunction<Input, Output> select,
          Operator<Input> from
  ) {
    return new Operator.Grouping<>(
            from.map(outputClass, select).newKeyedBuilder(item -> item).timed(true).build(),
            2,
            false
    ).flatMap(outputClass, group -> group.size() == 1 ? Stream.of(group.get(0)) : Stream.empty());
  }

  private static <Left, Right, On> Operator<Tuple2<List<Left>, List<Right>>> selectJoinOn(
          Operator<Left> leftFrom,
          Operator<Right> rightFrom,
          SerializableFunction<Left, On> leftOn,
          SerializableFunction<Right, On> rightOn
  ) {
    final var eitherClass = (Class<Either<Left, Right>>) (Class<?>) Either.class;
    final var inputLabel = new Operator.Input<>(eitherClass)
            .link(leftFrom.map(eitherClass, scala.util.Left::new))
            .link(rightFrom.map(eitherClass, scala.util.Right::new));
    return new Operator.Grouping<>(
            inputLabel.newKeyedBuilder(either -> either.fold(toScala(leftOn), toScala(rightOn))).timed(true).build(),
            Integer.MAX_VALUE,
            true
    ).flatMap((Class<Tuple2<List<Left>, List<Right>>>) (Class<?>) Tuple2.class, group ->
            group.stream().anyMatch(Either::isLeft) && group.stream().anyMatch(Either::isRight) ?
                    Stream.of(new Tuple2<>(
                            group.stream().flatMap(either ->
                                    either.fold(toScala(Stream::of), toScala(__ -> Stream.empty()))
                            ).collect(Collectors.toList()),
                            group.stream().flatMap(either ->
                                    either.fold(toScala(__ -> Stream.empty()), toScala(Stream::of))
                            ).collect(Collectors.toList())
                    )) : Stream.empty()
    );
  }

  private static <In, Out> AbstractFunction1<In, Out> toScala(SerializableFunction<In, Out> function) {
    return new AbstractFunction1<>() {
      @Override
      public Out apply(In in) {
        return function.apply(in);
      }
    };
  }

  public static long tumbleStart(Instant dateTime, long interval) {
    return (dateTime.getEpochSecond() / interval + 1) * interval;
  }
}
