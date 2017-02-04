package experiments.interfaces.nikita.akka.cluster.worker;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.cluster.Cluster;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by marnikitta on 2/2/17.
 */
public class WorkerApp {
  public static void main(String[] args) {
    final String[] ports = args.length > 0 ? args : new String[]{"2553"};
    for (String port : ports) {
      start(port);
    }
  }

  private static void start(String port) {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port)
            .withFallback(ConfigFactory.parseString("akka.cluster.roles = [worker]"))
            .withFallback(ConfigFactory.load("cluster"));
    final ActorSystem system = ActorSystem.create("System", config);

    system.actorOf(Props.create(Uppercaser.class), "uppercaser");

    //Graceful System shutdownHook
    Cluster.get(system).registerOnMemberRemoved(() -> {
      system.registerOnTermination(() -> System.exit(0));
      system.terminate();

      new Thread() {
        @Override
        public void run() {
          try {
            Await.ready(system.whenTerminated(), Duration.create(10, TimeUnit.SECONDS));
          } catch (Exception e) {
            System.exit(-1);
          }
        }
      }.start();
    });
  }
}
