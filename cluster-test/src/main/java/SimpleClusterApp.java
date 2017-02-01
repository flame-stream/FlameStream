import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by marnikitta on 2/1/17.
 */

public class SimpleClusterApp {

  public static void main(String[] args) {
    Config config = ConfigFactory.parseString(
            "akka.remote.netty.tcp.port=" + 2552).withFallback(
            ConfigFactory.load());

    ActorSystem system = ActorSystem.create("ClusterSystem", config);

    // Create an actor that handles cluster domain events
    system.actorOf(Props.create(ClusterListener.class),
            "clusterListener");
  }
}
