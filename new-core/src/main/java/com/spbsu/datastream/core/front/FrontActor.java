package com.spbsu.datastream.core.front;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.InPort;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.spbsu.datastream.core.range.RangeConciergeApi.DeployForTick;

public final class FrontActor extends LoggingActor {
  private final ActorRef rootRouter;

  private final int id;

  private final Map<Long, ActorRef> tickFronts = new HashMap<>();

  public static Props props(final ActorRef remoteRouter, final int id) {
    return Props.create(FrontActor.class, remoteRouter, id);
  }

  private FrontActor(final ActorRef rootRouter, final int id) {
    this.rootRouter = rootRouter;
    this.id = id;
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof RawData) {
      final Object data = ((RawData<?>) message).payload();

      final GlobalTime globalTime = new GlobalTime(System.currentTimeMillis(), this.id);
      final Meta now = new Meta(globalTime);
      final DataItem<?> dataItem = new PayloadDataItem<>(now, data);

      final long tick = globalTime.time() / TimeUnit.HOURS.toMillis(1);

      final ActorRef tickFront = this.tickFronts.get(tick);
      tickFront.tell(dataItem, ActorRef.noSender());
    } else if (message instanceof DeployForTick) {
      final DeployForTick deploy = (DeployForTick) message;
      this.LOG().info("Deploying for tick: {}", deploy);

      final InPort target = deploy.graph().frontBindings().get(this.id);

      final ActorRef tickFront = this.context().actorOf(TickFrontActor.props(this.rootRouter, target, deploy.tick()),
              Long.toString(deploy.tick()));

      this.tickFronts.putIfAbsent(deploy.tick(), tickFront);
    } else {
      this.unhandled(message);
    }
  }
}
