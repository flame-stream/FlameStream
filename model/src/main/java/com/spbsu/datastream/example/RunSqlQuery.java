package com.spbsu.datastream.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.akka.SimpleAkkaSink;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.exceptions.InvalidQueryException;
import com.spbsu.datastream.core.exceptions.UnsupportedQueryException;
import com.spbsu.datastream.core.inference.sql.SqlInference;
import com.spbsu.datastream.core.io.Output;
import com.spbsu.datastream.core.job.Joba;
import com.spbsu.datastream.core.job.control.EndOfTick;
import com.spbsu.datastream.example.bl.UserContainer;

/**
 * Created by Artem on 15.11.2016.
 */
@SuppressWarnings("WeakerAccess")
public class RunSqlQuery {
  public static void main(String[] args) {
    final SqlInference sqlInference = DataStreamsContext.sqlInference;
    final ActorSystem akka = ActorSystem.create();

    DataStreamsContext.input.stream(sqlInference.type("UsersLog")).flatMap((input) -> {
      final Joba joba;
      try {
        final SimpleAkkaSink<DataItem> sink = new SimpleAkkaSink<>(DataItem.class, EndOfTick.class::isInstance);
        joba = sqlInference.query("SELECT * FROM UsersLog WHERE user = 'petya'", UserContainer.class);
        final ActorRef materialize = joba.materialize(akka, sink.actor(akka));
        new Thread(() -> {
          input.forEach(di -> {
            materialize.tell(di, ActorRef.noSender());
            Thread.yield();
          });
          materialize.tell(new EndOfTick(), ActorRef.noSender());
        }).start();
        return sink.stream().onClose(() -> Output.instance().commit());
      } catch (InvalidQueryException | UnsupportedQueryException e) {
        throw new RuntimeException(e);
      }
    }).forEach(Output.instance().printer());
    akka.shutdown();
  }

}