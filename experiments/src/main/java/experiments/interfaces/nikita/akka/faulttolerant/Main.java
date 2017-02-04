package experiments.interfaces.nikita.akka.faulttolerant;

import akka.actor.ActorSystem;
import akka.actor.ReceiveTimeout;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.duration.Duration;

/**
 * Created by marnikitta on 2/1/17.
 */
public class Main {
  public static void main(String[] args) {
    Config config = ConfigFactory.parseString("akka.loglevel = DEBUG \n" +
            "akka.actor.debug.lifecycle = on");
    ActorSystem system = ActorSystem.create("FaultToleranceSample", config);


  }

  public static class Progress {
    public final double percent;

    public Progress(double percent) {
      this.percent = percent;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("Progress{");
      sb.append("percent=").append(percent);
      sb.append('}');
      return sb.toString();
    }
  }

  public static class Listener extends UntypedActor {
    final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    @Override
    public void preStart() {
      getContext().setReceiveTimeout(Duration.create("15 seconds"));
    }

    public void onReceive(Object msg) {
      log.debug("received message {}", msg);
      if (msg instanceof Progress) {
        Progress progress = (Progress) msg;
        log.info("Current progress: {} %", progress.percent);
        if (progress.percent >= 100.0) {
          log.info("That’s all, shutting down");
          getContext().system().terminate();
        }
      } else if (msg == ReceiveTimeout.getInstance()) {
        log.error("Shutting down due to unavailable service");
        getContext().system().terminate();
      } else {
        unhandled(msg);
      }
    }
  }
}
