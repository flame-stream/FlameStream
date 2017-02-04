package com.spbsu.datastream.example.usercounter;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.akka.ActorContainer;
import com.spbsu.commons.func.types.ConversionRepository;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.impl.TypeConvertersCollection;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.system.RuntimeUtils;
import com.spbsu.datastream.core.*;
import com.spbsu.datastream.core.io.UserLogInput;
import com.spbsu.datastream.core.job.FilterJoba;
import com.spbsu.datastream.core.job.GroupingJoba;
import com.spbsu.datastream.core.job.MergeActor;
import com.spbsu.datastream.core.job.ReplicatorJoba;
import com.spbsu.datastream.core.job.control.EndOfTick;

import java.util.function.Function;


public class RunUserCounter {
  public static void main(String[] args) {
    DataStreamsContext.serializatonRepository = new SerializationRepository<>(
            new TypeConvertersCollection(ConversionRepository.ROOT,
                    UserContainer.class.getPackage().getName() + ".io"),
            CharSeq.class
    );
    final DataTypeCollection types = (DataTypeCollection) DataStreamsContext.typeCollection;
    final ActorSystem akka = ActorSystem.create();
    final int maxUserCount = 5000;

    new UserLogInput().stream(/*"ypes.type("UsersLog")*/ null).flatMap((input) -> {
      StreamSink sink = new StreamSink();
      //joba = types.<Integer>convert(types.type("UsersLog"), types.type("Group(UserLog, 2)"), sink);
      Sink joba = makeJoba(akka, sink, types);
      new Thread(() -> {
        //Thread.yield();
        input.forEach(joba::accept);
        joba.accept(new EndOfTick());
      }).start();
      return sink.stream().onClose(DataStreamsContext.output::commit);
    }).forEach(DataStreamsContext.output.processor());

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