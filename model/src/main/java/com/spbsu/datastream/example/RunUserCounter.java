package com.spbsu.datastream.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.akka.SimpleAkkaSink;
import com.spbsu.datastream.core.Condition;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.exceptions.TypeUnreachableException;
import com.spbsu.datastream.core.inference.DataTypeCollection;
import com.spbsu.datastream.core.io.Output;
import com.spbsu.datastream.core.job.IndicatorJoba;
import com.spbsu.datastream.core.job.Joba;
import com.spbsu.datastream.core.job.control.EndOfTick;
import com.spbsu.datastream.example.bl.counter.UserCounter;
import com.spbsu.datastream.example.bl.counter.UserMaxCountCondition;

/**
 * Experts League
 * Created by solar on 27.10.16.
 */
@SuppressWarnings("WeakerAccess")
public class RunUserCounter {
  public static void main(String[] args) {
    final DataTypeCollection types = DataStreamsContext.typeCollection;
    final ActorSystem akka = ActorSystem.create();

    final int maxUserCount = 5000;
    final Condition<UserCounter> condition = new UserMaxCountCondition(maxUserCount);

    DataStreamsContext.input.stream(types.type("UsersLog")).flatMap((input) -> {
      final Joba joba;
      try {
        final SimpleAkkaSink<DataItem> sink = new SimpleAkkaSink<>(DataItem.class, EndOfTick.class::isInstance);
        joba = new IndicatorJoba(types.<Integer>convert(types.type("UsersLog"), types.type("Frequencies")), condition);
        final ActorRef materialize = joba.materialize(akka, sink.actor(akka));
        new Thread(() -> {
          input.forEach(di -> {
            materialize.tell(di, ActorRef.noSender());
            Thread.yield();
          });
          materialize.tell(new EndOfTick(), ActorRef.noSender());
        }).start();
        return sink.stream().onClose(() -> Output.instance().commit());
      } catch (TypeUnreachableException tue) {
        throw new RuntimeException(tue);
      }
    }).forEach(Output.instance().printer());
    akka.shutdown();
  }

}
