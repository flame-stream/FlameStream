package experiments.interfaces.artem.impl;

import experiments.interfaces.artem.Condition;
import experiments.interfaces.artem.DataItem;
import experiments.interfaces.artem.DataStream;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public class MergeStream implements DataStream {
  public MergeStream(DataStream... streams) {

  }

  @Override
  public Type types() {
    return null;
  }

  @Override
  public boolean isValid() {
    return false;
  }

  @Override
  public double alpha() {
    return 0;
  }

  @Override
  public Set<Condition> violated() {
    return null;
  }

  @Override
  public Stream<DataItem> filter(Predicate<? super DataItem> predicate) {
    return null;
  }

  @Override
  public <R> Stream<R> map(Function<? super DataItem, ? extends R> mapper) {
    return null;
  }

  @Override
  public IntStream mapToInt(ToIntFunction<? super DataItem> mapper) {
    return null;
  }

  @Override
  public LongStream mapToLong(ToLongFunction<? super DataItem> mapper) {
    return null;
  }

  @Override
  public DoubleStream mapToDouble(ToDoubleFunction<? super DataItem> mapper) {
    return null;
  }

  @Override
  public <R> Stream<R> flatMap(Function<? super DataItem, ? extends Stream<? extends R>> mapper) {
    return null;
  }

  @Override
  public IntStream flatMapToInt(Function<? super DataItem, ? extends IntStream> mapper) {
    return null;
  }

  @Override
  public LongStream flatMapToLong(Function<? super DataItem, ? extends LongStream> mapper) {
    return null;
  }

  @Override
  public DoubleStream flatMapToDouble(Function<? super DataItem, ? extends DoubleStream> mapper) {
    return null;
  }

  @Override
  public Stream<DataItem> distinct() {
    return null;
  }

  @Override
  public Stream<DataItem> sorted() {
    return null;
  }

  @Override
  public Stream<DataItem> sorted(Comparator<? super DataItem> comparator) {
    return null;
  }

  @Override
  public Stream<DataItem> peek(Consumer<? super DataItem> action) {
    return null;
  }

  @Override
  public Stream<DataItem> limit(long maxSize) {
    return null;
  }

  @Override
  public Stream<DataItem> skip(long n) {
    return null;
  }

  @Override
  public void forEach(Consumer<? super DataItem> action) {

  }

  @Override
  public void forEachOrdered(Consumer<? super DataItem> action) {

  }

  @Override
  public Object[] toArray() {
    return new Object[0];
  }

  @Override
  public <A> A[] toArray(IntFunction<A[]> generator) {
    return null;
  }

  @Override
  public DataItem reduce(DataItem identity, BinaryOperator<DataItem> accumulator) {
    return null;
  }

  @Override
  public Optional<DataItem> reduce(BinaryOperator<DataItem> accumulator) {
    return null;
  }

  @Override
  public <U> U reduce(U identity, BiFunction<U, ? super DataItem, U> accumulator, BinaryOperator<U> combiner) {
    return null;
  }

  @Override
  public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super DataItem> accumulator, BiConsumer<R, R> combiner) {
    return null;
  }

  @Override
  public <R, A> R collect(Collector<? super DataItem, A, R> collector) {
    return null;
  }

  @Override
  public Optional<DataItem> min(Comparator<? super DataItem> comparator) {
    return null;
  }

  @Override
  public Optional<DataItem> max(Comparator<? super DataItem> comparator) {
    return null;
  }

  @Override
  public long count() {
    return 0;
  }

  @Override
  public boolean anyMatch(Predicate<? super DataItem> predicate) {
    return false;
  }

  @Override
  public boolean allMatch(Predicate<? super DataItem> predicate) {
    return false;
  }

  @Override
  public boolean noneMatch(Predicate<? super DataItem> predicate) {
    return false;
  }

  @Override
  public Optional<DataItem> findFirst() {
    return null;
  }

  @Override
  public Optional<DataItem> findAny() {
    return null;
  }

  @Override
  public Iterator<DataItem> iterator() {
    return null;
  }

  @Override
  public Spliterator<DataItem> spliterator() {
    return null;
  }

  @Override
  public boolean isParallel() {
    return false;
  }

  @Override
  public Stream<DataItem> sequential() {
    return null;
  }

  @Override
  public Stream<DataItem> parallel() {
    return null;
  }

  @Override
  public Stream<DataItem> unordered() {
    return null;
  }

  @Override
  public Stream<DataItem> onClose(Runnable closeHandler) {
    return null;
  }

  @Override
  public void close() {

  }
}
