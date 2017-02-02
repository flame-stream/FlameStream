package experiments.interfaces.nikita.akka.cluster.master;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.cluster.sharding.ClusterSharding;
import akka.cluster.sharding.ClusterShardingSettings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import experiments.interfaces.nikita.akka.cluster.Envelope;
import experiments.interfaces.nikita.akka.cluster.MyMessageExtractor;
import experiments.interfaces.nikita.akka.cluster.worker.Uppercaser;

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
            .withFallback(ConfigFactory.load());
    final ActorSystem system = ActorSystem.create("System", config);

//    final ClusterShardingSettings settings = ClusterShardingSettings.create(system).withRole("worker");
//    final ActorRef shardRegion = ClusterSharding.get(system)
//            .start("Counter", Props.create(Uppercaser.class), settings, new MyMessageExtractor());
//
//    for (int i = 0; i < 100; ++i) {
//      shardRegion.tell(new Envelope(i, "abacaba"), null);
//    }

//    final ActorRef ref = system.actorOf(Props.create(MasterPublisher.class));

//    system.scheduler().schedule(Duration.create(1, TimeUnit.SECONDS),
//            Duration.create(5, TimeUnit.SECONDS),
//            ref,
//            "tick",
//            system.dispatcher(),
//            null
//    );
  }
}
