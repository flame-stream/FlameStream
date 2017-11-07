package com.spbsu.flamestream.runtime.froontt;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.front.FrontSubscription;
import com.spbsu.flamestream.core.front.SourceHandle;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.source.RequestMore;

public class FrontSubscriber<T> implements SourceHandle<T> {
  private final ActorRef actor;

  FrontSubscriber() {
    //create actor
    this.actor = null;
  }

  @Override
  public void onSubscribe(FrontSubscription subscription) {
    actor.tell(subscription, ActorRef.noSender());
  }

  @Override
  public void accept(DataItem<T> dataItem) {
    actor.tell(dataItem, ActorRef.noSender());
  }

  @Override
  public void submit(GlobalTime watermark) {
    actor.tell(watermark, ActorRef.noSender());
  }

  @Override
  public void onError(Throwable throwable) {
    actor.tell(throwable, ActorRef.noSender());
  }

  @Override
  public void onComplete() {
    actor.tell(1, ActorRef.noSender());
  }
}

final class FrontSubscriberActor extends LoggingActor {
  private final FrontSubscription subscription;

  private ActorRef consumer;

  public FrontSubscriberActor(FrontSubscription subscription) {
    this.subscription = subscription;
    resolveConsumer();
  }

  private void resolveConsumer() {
  }

  public static Props props(FrontSubscription subscription) {
    return Props.create(FrontSubscriberActor.class, subscription);
  }


  @Override
  public void preStart() throws Exception {
    super.preStart();
  }
  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(RequestMore.class, e -> subscription.request(e.count()))
            .match(DataItem.class, dataItem -> consumer.tell(dataItem, self()))
            .build();
  }

}
