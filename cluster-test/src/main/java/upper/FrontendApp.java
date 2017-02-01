package upper;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by marnikitta on 2/1/17.
 */
public class FrontendApp {
  public static void main(String[] args) {
    final Config config = ConfigFactory.parseString(
            "akka.cluster.roles = [frontend]").withFallback(
            ConfigFactory.load("application"));

    final ActorSystem system = ActorSystem.create("ClusterSystem", config);
    system.actorOf(Props.create(Frontend.class), "frontend");
  }
}
