package experiments.interfaces.solar;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.akka.SimpleAkkaSink;
import experiments.interfaces.solar.control.EndOfTick;

/**
 * Experts League
 * Created by solar on 27.10.16.
 */
@SuppressWarnings("WeakerAccess")
public class Main {
  public static void main(String[] args) {
    final DataTypeCollection types = DataTypeCollection.instance();
    final ActorSystem akka = ActorSystem.create();
    Input.instance().stream(types.type("UsersLog")).flatMap((input) -> {
      final Joba joba;
      try {
        final SimpleAkkaSink<DataItem> sink = new SimpleAkkaSink<>(DataItem.class, o -> o instanceof EndOfTick);
        joba = types.<Integer>convert(types.type("UsersLog"), types.type("Frequences"));
        final ActorRef materialize = joba.materialize(akka, sink.actor(akka));
        new Thread(() -> {
          input.forEach(di -> materialize.tell(di, ActorRef.noSender()));
          materialize.tell(new EndOfTick(), ActorRef.noSender());
        }).start();
        return sink.stream().onClose(() -> Output.instance().commit());
      }
      catch (TypeUnreachableException tue) {
        throw new RuntimeException(tue);
      }
    }).forEach(Output.instance().printer());
    akka.shutdown();
  }

}
