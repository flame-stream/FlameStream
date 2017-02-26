package com.spbsu.datastream.core.materializer;

import akka.actor.UntypedActor;
import com.spbsu.datastream.core.DataItem;

/**
 * Created by marnikitta on 2/8/17.
 */
public class AtomicActor extends UntypedActor {
  private final ShardConcierge concierge;
  private final GraphStageLogic logic;

  private AtomicActor(final GraphStageLogic logic, final ShardConcierge concierge) {
    this.concierge = concierge;
    this.logic = logic;
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof DataItem) {
    }
  }
}
