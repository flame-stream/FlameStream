package com.spbsu.akka;

import akka.actor.Actor;
import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.commons.func.Action;

/**
 * @author vpdelta
 */
public abstract class ActorAdapter<A extends Actor> {
  protected A actor;
  protected Action<Object> unhandled;

  public ActorAdapter() {
    // to make it possible to create adapter outside of actor system (in tests)
  }

  public final void injectActor(final A actor) {
    this.actor = actor;
    this.unhandled = actor::unhandled;
    init();
  }

  public final void injectUnhandled(final Action<Object> unhandled) {
    this.unhandled = unhandled;
  }

  protected void init() {
  }

  protected void preStart() throws Exception {
  }

  protected void postStop() {
  }

  // todo: in future we can have complete delegation here
  public ActorRef self() {
    return getActor().self();
  }

  protected A getActor() {
    if (actor == null) {
      throw new IllegalStateException("Actor is not injected");
    }
    return actor;
  }

  public ActorRef sender() {
    return getActor().sender();
  }

  public ActorContext context() {
    return getActor().context();
  }

  public void unhandled(Object message) {
    this.unhandled.invoke(message);
  }

  public void reply(final Object message) {
    replyTo(sender(), message);
  }

  public void replyTo(ActorRef recipient, final Object message) {
    recipient.tell(message, self());
  }

  public ActorRef actorOf(final Class<? extends ActorAdapter> adapter, final Object... args) {
//    if (PersistentActorAdapter.class.isAssignableFrom(adapter)) {
//      return context().actorOf(PersistentActorContainer.props(adapter, args));
//    }
//    else {
    return context().actorOf(ActorContainer.props(adapter, args));
//    }
  }
}
