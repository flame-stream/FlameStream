package upper;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by marnikitta on 2/1/17.
 */
public class BackendApp {

  public static void main(String[] args) {
    final String port = args.length > 0 ? args[0] : "2552";
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port).
            withFallback(ConfigFactory.parseString("akka.cluster.roles = [backend]")).
            withFallback(ConfigFactory.load("application"));

    ActorSystem system = ActorSystem.create("ClusterSystem", config);

    system.actorOf(Props.create(Backend.class), "upperBackend");
  }
}
