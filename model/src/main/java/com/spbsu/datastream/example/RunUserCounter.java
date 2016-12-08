package com.spbsu.datastream.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.akka.SimpleAkkaSink;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.inference.JobaBuilder;
import com.spbsu.datastream.core.inference.SimpleBuilder;
import com.spbsu.datastream.core.inference.TypeCollection;
import com.spbsu.datastream.core.io.Output;
import com.spbsu.datastream.core.job.Joba;
import com.spbsu.datastream.core.job.control.EndOfTick;

import javax.inject.Inject;
import java.util.stream.Stream;

/**
 * Experts League
 * Created by solar on 27.10.16.
 */
@SuppressWarnings("WeakerAccess")
public class RunUserCounter {
//  @Inject
//  final TypeCollection types = DataStreamsContext.typeCollection;
//
//  @Inject
//  final JobaBuilder builder = DataStreamsContext.jobaBuilder;
//
//  public static void main(String[] args) {
//    new RunUserCounter().run();
//  }
//
//  public void run() {
//    types.addMorphism(new UserCountMorphism());
//    ((SimpleBuilder) builder).index();
//
//    final ActorSystem akka = ActorSystem.create();
//    Stream<Stream<DataItem>> input = DataStreamsContext.input.stream(types.type("UsersLog"));
//
//    input.flatMap((tickStream) -> {
//      final Joba joba = builder.build(types.type("UsersLog"), types.type("Frequencies"));
//
//      final SimpleAkkaSink<DataItem> sink = new SimpleAkkaSink<>(DataItem.class, EndOfTick.class::isInstance);
//
//      final ActorRef materialize = joba.materialize(akka, sink.actor(akka));
//      new Thread(() -> {
//        tickStream.forEach(di -> {
//          materialize.tell(di, ActorRef.noSender());
//          Thread.yield();
//        });
//        materialize.tell(new EndOfTick(), ActorRef.noSender());
//      }).start();
//      return sink.stream().onClose(() -> Output.instance().commit());
//    }).forEach(Output.instance().printer());
//
//    akka.shutdown();
//  }
}
