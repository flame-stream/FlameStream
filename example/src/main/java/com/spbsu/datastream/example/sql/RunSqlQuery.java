package com.spbsu.datastream.example.sql;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.akka.ActorContainer;
import com.spbsu.datastream.core.ActorSink;
import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.StreamSink;
import com.spbsu.datastream.core.job.MergeActor;
import com.spbsu.datastream.core.job.control.EndOfTick;
import com.spbsu.datastream.example.usercounter.UserContainer;
import com.spbsu.datastream.sql.exceptions.InvalidQueryException;
import com.spbsu.datastream.sql.exceptions.UnsupportedQueryException;
import com.spbsu.datastream.sql.inference.SqlInference;

/**
 * Created by Artem on 15.11.2016.
 */
@SuppressWarnings("WeakerAccess")
public class RunSqlQuery {
  public static void main(String[] args) {
    final SqlInference sqlInference = new SqlInference();

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
        return streamSink.stream().onClose(DataStreamsContext.output::commit);
      } catch (InvalidQueryException | UnsupportedQueryException e) {
        throw new RuntimeException(e);
      }
    }).forEach(DataStreamsContext.output.processor());

    akka.shutdown();
  }

}