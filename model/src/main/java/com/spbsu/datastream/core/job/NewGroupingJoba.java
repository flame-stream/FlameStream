package com.spbsu.datastream.core.job;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import com.spbsu.akka.ActorAdapter;
import com.spbsu.akka.ActorContainer;
import com.spbsu.akka.ActorMethod;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.item.ListDataItem;
import com.spbsu.datastream.core.job.control.Control;
import com.spbsu.datastream.core.job.control.EndOfTick;
import com.spbsu.datastream.core.state.GroupingState;
import com.spbsu.datastream.core.state.StateRepository;

import java.util.Collections;
import java.util.Comparator;
import java.util.Optional;

public class NewGroupingJoba extends Joba.Stub {
  private final StateRepository stateRepository = DataStreamsContext.stateRepository;

  private final DataItem.Grouping grouping;

  private final int window;

  public NewGroupingJoba(Joba base, DataType generates, DataItem.Grouping grouping, int window) {
    super(generates);
    this.grouping = grouping;
    this.window = window;
  }

  @Override
  public void accept(DataItem item) {

  }

  @Override
  public void accept(Control control) {

  }

  @SuppressWarnings({"WeakerAccess", "unused"})
  public static class GroupingActor extends ActorAdapter<UntypedActor> {

    private final GroupingState buffer = new GroupingState();
    private final ActorRef sink;
    private final NewGroupingJoba padre;
    boolean eos = false;

    public GroupingActor(NewGroupingJoba padre, ActorRef sink) {
      this.padre = padre;
      this.sink = sink;
    }

    @ActorMethod
    public void group(DataItem item) {
      final long hash = padre.grouping.hash(item);
      final int window = padre.window;
      final int jobaId = padre.id();

      final Optional<GroupingState.Bucket> bucketOpt = buffer.searchBucket(hash, item, padre.grouping);

      if (bucketOpt.isPresent()) { // look for time collision in the current tick
        final GroupingState.Bucket group = bucketOpt.get();
        int expectedPosition = -(Collections.binarySearch(group, item, Comparator.comparing(DataItem::meta)) + 1);

        group.add(expectedPosition, item);

        for (int i = expectedPosition; i < group.size(); i++) {
          sink.tell(new ListDataItem(group.subList(Math.max(0, i - window), i + 1), group.get(i).meta()), self());
        }
      } else { // creating group from existing in the state
        final GroupingState state = padre.stateRepository.load(padre.generates().name());
        final GroupingState.Bucket stateBucket = state.searchBucket(hash, item, padre.grouping)
                .map(GroupingState.Bucket::new)
                .orElse(new GroupingState.Bucket());

        stateBucket.add(item);
        buffer.putBucket(hash, stateBucket);
        sink.tell(new ListDataItem(stateBucket.subList(Math.max(0, stateBucket.size() - window), stateBucket.size()), item.meta()), self());
      }
    }

    @ActorMethod
    public void control(Control eot) {
      sink.tell(eot, sender());

      if (eot instanceof EndOfTick) {
        padre.stateRepository.update(padre.generates().name(), stateOpt -> {
          final GroupingState state = stateOpt.orElse(new GroupingState());

          buffer.forEach((bucket, hash) -> {
            final int window = padre.window;

            final GroupingState.Bucket windowedGroup = new GroupingState.Bucket(bucket.subList(Math.max(0, bucket.size() - window), bucket.size()));
            final Optional<GroupingState.Bucket> oldGroup = state.searchBucket(hash, bucket.get(0), padre.grouping);

            if (oldGroup.isPresent()) {
              oldGroup.get().clear();
              oldGroup.get().addAll(windowedGroup);
            } else {
              state.putBucket(hash, windowedGroup);
            }
          });
          return state;
        });

        context().stop(self());
      }
    }
  }
}
