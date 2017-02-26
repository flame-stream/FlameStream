package experiments.nikita.akka.cluster.master;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by marnikitta on 2/2/17.
 */
public class MasterApp {
  public static void main(String[] args) {
    final String[] ports = args.length > 0 ? args : new String[]{"2551"};
    for (String port : ports) {
      start(port);
    }
  }

  private static void start(String port) {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port)
            .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
            .withFallback(ConfigFactory.load("cluster"));
    final ActorSystem system = ActorSystem.create("System", config);


    final ActorRef ref = system.actorOf(Props.create(MasterActor.class));

    system.scheduler().schedule(Duration.create(1, TimeUnit.SECONDS),
            Duration.create(5, TimeUnit.SECONDS),
            ref,
            "tick",
            system.dispatcher(),
            null
    );
  }
}
