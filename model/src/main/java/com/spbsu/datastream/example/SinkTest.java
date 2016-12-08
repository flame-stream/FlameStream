package com.spbsu.datastream.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.akka.ActorContainer;
import com.spbsu.commons.system.RuntimeUtils;
import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.MergeActor;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.StreamSink;
import com.spbsu.datastream.core.inference.DataTypeCollection;
import com.spbsu.datastream.core.io.Output;
import com.spbsu.datastream.core.job.ActorSink;
import com.spbsu.datastream.core.job.FilterJoba;
import com.spbsu.datastream.core.job.GroupingJoba;
import com.spbsu.datastream.core.job.ReplicatorJoba;
import com.spbsu.datastream.core.job.control.EndOfTick;
import com.spbsu.datastream.example.bl.UserGrouping;
import com.spbsu.datastream.example.bl.counter.CountUserEntries;
import com.spbsu.datastream.example.bl.counter.UserCounter;

import java.util.function.Function;


public class SinkTest {
  public static void main(String[] args) {
    final DataTypeCollection types = (DataTypeCollection) DataStreamsContext.typeCollection;
    final ActorSystem akka = ActorSystem.create();
    final int maxUserCount = 5000;

    DataStreamsContext.input.stream(/*"ypes.type("UsersLog")*/ null).flatMap((input) -> {
      StreamSink sink = new StreamSink();
      //joba = types.<Integer>convert(types.type("UsersLog"), types.type("Group(UserLog, 2)"), sink);
      Sink joba = makeJoba(akka, sink, types);
      new Thread(() -> {
        //Thread.yield();
        input.forEach(joba::accept);
        joba.accept(new EndOfTick());
      }).start();
      return sink.stream().onClose(() -> Output.instance().commit());
    }).forEach(Output.instance().printer());

    akka.shutdown();
  }

  public static Sink makeJoba(ActorSystem actorSystem, Sink sink, DataTypeCollection types) {
    final ReplicatorJoba replicator = new ReplicatorJoba(sink);
    final FilterJoba states = new FilterJoba(replicator, null, new CountUserEntries(), RuntimeUtils.findTypeParameters(CountUserEntries.class, Function.class)[0], UserCounter.class);
    final GroupingJoba grouping = new GroupingJoba(states, types.type("Group(Merge(UsersLog, States), UserHash, 2)"), new UserGrouping(), 2);
    ActorRef mergeActor = actorSystem.actorOf(ActorContainer.props(MergeActor.class, grouping, 2));
    ActorSink mergeSink = new ActorSink(mergeActor);
    replicator.add(mergeSink);
    return mergeSink;
  }

}