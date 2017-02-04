package com.spbsu.akka;

import akka.actor.Props;
import akka.actor.UntypedActor;

/**
 * @author vpdelta
 */
public class ActorContainer extends UntypedActor {
  private final ActorInvokeDispatcher<ActorAdapter> dispatcher;

  public static Props props(final Class<? extends ActorAdapter> adapter, final Object... args) {
    return Props.create(ActorContainer.class, new Object[]{new AdapterProps[]{AdapterProps.create(adapter, args)}});
  }

  public static Props props(
    final AdapterProps adapterProps,
    final AdapterProps overrideProps
  ) {
    return Props.create(ActorContainer.class, new Object[]{new AdapterProps[]{adapterProps, overrideProps}});
  }

  public ActorContainer(AdapterProps[] props) {
    dispatcher = new ActorInvokeDispatcher<>(this, props, this::unhandled);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    getAdapterInstance().preStart();
  }

  @Override
  public void postStop() throws Exception {
    getAdapterInstance().postStop();
    super.postStop();
  }

  @Override
  public void onReceive(final Object message) throws Exception {
    if (ActorFailureChecker.checkIfFailure(getAdapterInstance().getClass(), self().path().name(), message)) {
      return;
    }

    MessageCapture.instance().capture(sender(), self(), message);

    dispatcher.invoke(message);
  }

  private ActorAdapter getAdapterInstance() {
    return dispatcher.getDispatchSequence().get(0).getInstance();
  }
}
