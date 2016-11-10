package com.spbsu.akka;

import akka.actor.UntypedActor;
import com.spbsu.commons.system.RuntimeUtils;

import java.util.logging.Logger;

/**
 * User: solar
 * Date: 03.12.15
 * Time: 15:57
 */
public abstract class UntypedActorAdapter extends UntypedActor {
  private static final Logger log = Logger.getLogger(UntypedActorAdapter.class.getName());
  private RuntimeUtils.InvokeDispatcher dispatcher;

  public UntypedActorAdapter() {
    dispatcher = new RuntimeUtils.InvokeDispatcher(getClass(), this::unhandled);
  }

  @Override
  public final void onReceive(Object message) throws Exception {
    if (ActorFailureChecker.checkIfFailure(getClass(), self().path().name(), message)) {
      return;
    }

    MessageCapture.instance().capture(sender(), self(), message);

    dispatcher.invoke(this, message);
  }
}
