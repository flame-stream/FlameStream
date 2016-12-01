package com.spbsu.datastream.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.akka.ActorContainer;
import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.MergeActor;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.StreamSink;
import com.spbsu.datastream.core.exceptions.InvalidQueryException;
import com.spbsu.datastream.core.exceptions.UnsupportedQueryException;
import com.spbsu.datastream.core.inference.sql.SqlInference;
import com.spbsu.datastream.core.io.Output;
import com.spbsu.datastream.core.job.ActorSink;
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
      final StreamSink streamSink = new StreamSink();
      try {
        Sink queryJoba = sqlInference.query("SELECT * FROM UsersLog WHERE user = 'petya' LIMIT 5", streamSink, UserContainer.class);
        ActorRef mergeActor = akka.actorOf(ActorContainer.props(MergeActor.class, queryJoba, 1));
        ActorSink actorSink = new ActorSink(mergeActor);
        new Thread(() -> {
          input.forEach(actorSink::accept);
          actorSink.accept(new EndOfTick());
        }).start();
        return streamSink.stream().onClose(() -> Output.instance().commit());
      } catch (InvalidQueryException | UnsupportedQueryException e) {
        throw new RuntimeException(e);
      }
    }).forEach(Output.instance().printer());

    akka.shutdown();
  }

}