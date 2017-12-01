package com.spbsu.flamestream.runtime;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.Rear;
import com.spbsu.flamestream.runtime.edge.front.BalancingActor;
import com.spbsu.flamestream.runtime.edge.front.RemoteActorFront;
import com.spbsu.flamestream.runtime.edge.rear.ActorRear;
import com.spbsu.flamestream.runtime.edge.rear.ConsumerActor;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class TestRuntime implements FlameRuntime {
  private final LocalRuntime runtime;

  public TestRuntime(int parallelism) {
    this.runtime = new LocalRuntime(parallelism);
  }

  @Override
  public TestFlame run(Graph g) {
    return new TestFlame(runtime.run(g));
  }

  public class TestFlame implements Flame {
    private final Flame superFlame;

    public TestFlame(Flame superFlame) {
      this.superFlame = superFlame;
    }

    @Override
    public void extinguish() {
      superFlame.extinguish();
    }

    public Consumer<Object> attachBalancingFront() {
      final ActorRef ref = runtime.system().actorOf(BalancingActor.props(), "balancer");
      final String path = "akka://" + runtime.system().name() + "/user/balancer";
      superFlame.attachFront("actor-front", RemoteActorFront.class, "actor-front", path);
      return o -> ref.tell(o, ActorRef.noSender());
    }

    public <T> void attachWrappedConsumer(Consumer<T> consumer) {
      final ActorRef ref = runtime.system().actorOf(ConsumerActor.props(consumer), "consumer");
      final String path = "akka://" + runtime.system().name() + "/user/consumer";
      superFlame.attachRear("actor-rear", ActorRear.class, path);
    }

    @Override
    public <T extends Front, H extends FrontHandle> Stream<H> attachFront(String id,
                                                                          Class<T> front,
                                                                          String... args) {
      return superFlame.attachFront(id, front, args);
    }

    @Override
    public <T extends Rear, H extends RearHandle> Stream<H> attachRear(String id,
                                                                       Class<T> rear,
                                                                       String... args) {
      return superFlame.attachRear(id, rear, args);
    }
  }
}
