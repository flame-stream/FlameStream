package com.spbsu.datastream.example.sql;

//import akka.actor.ActorRef;
//import akka.actor.ActorSystem;
//import com.spbsu.akka.SimpleAkkaSink;
//import com.spbsu.datastream.core.DataItem;
//import com.spbsu.datastream.core.DataStreamsContext;
//import com.spbsu.datastream.core.TypeNotSupportedException;
//import com.spbsu.datastream.core.inference.SqlInference;
//import com.spbsu.datastream.core.io.Output;
//import com.spbsu.datastream.core.job.IndicatorJoba;
//import com.spbsu.datastream.core.job.Joba;
//import com.spbsu.datastream.core.job.control.EndOfTick;
//import com.spbsu.datastream.example.bl.sql.UserFilterByName;

/**
 * Created by Artem on 15.11.2016.
 */
@SuppressWarnings("WeakerAccess")
public class RunUserSelector {
//  public static void main(String[] args) {
//    final SqlInference sqlInference = DataStreamsContext.sqlInference;
//    final ActorSystem akka = ActorSystem.create();
//
//    DataStreamsContext.input.stream(sqlInference.type("UsersLog")).flatMap((input) -> {
//      final Joba joba;
//      try {
//        final SimpleAkkaSink<DataItem> sink = new SimpleAkkaSink<>(DataItem.class, EndOfTick.class::isInstance);
//        joba = sqlInference.select(sqlInference.type("UsersLog"), new UserFilterByName("petya"));
//        final ActorRef materialize = joba.materialize(akka, sink.actor(akka));
//        new Thread(() -> {
//          input.forEach(di -> {
//            materialize.tell(di, ActorRef.noSender());
//            Thread.yield();
//          });
//          materialize.tell(new EndOfTick(), ActorRef.noSender());
//        }).start();
//        return sink.stream().onClose(() -> Output.instance().commit());
//      } catch (TypeNotSupportedException tnse) {
//        throw new RuntimeException(tnse);
//      }
//    }).forEach(Output.instance().printer());
//    akka.shutdown();
//  }

}