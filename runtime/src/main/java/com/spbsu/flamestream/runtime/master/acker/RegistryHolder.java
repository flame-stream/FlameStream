package com.spbsu.flamestream.runtime.master.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Commit;
import com.spbsu.flamestream.runtime.master.acker.api.commit.GimmeLastCommit;
import com.spbsu.flamestream.runtime.master.acker.api.commit.LastCommit;
import com.spbsu.flamestream.runtime.master.acker.api.registry.FrontTicket;
import com.spbsu.flamestream.runtime.master.acker.api.registry.RegisterFront;
import com.spbsu.flamestream.runtime.master.acker.api.registry.RegisterFrontFromTime;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


/**
 * <h3>Actor Contract</h3>
 * <h4>Inbound Messages</h4>
 * <ol>
 * <li>{@link RegisterFront} requests to add frontClass to the supervision</li>
 * </ol>
 * <h4>Outbound Messages</h4>
 * <ol>
 * <li>{@link FrontTicket} - reply to the frontClass registration request. Sets the lowest allowed timestamp</li>
 * <li>{@link MinTimeUpdate} mintime</li>
 * </ol>
 * <h4>Failure Modes</h4>
 * <ol>
 * <li>{@link RuntimeException} - if something goes wrong</li>
 * </ol>
 */
public class RegistryHolder extends LoggingActor {
  private static class NewFrontRegisterer extends LoggingActor {
    static class Registered {
      private final FrontTicket frontTicket;
      private final ActorRef sender;

      Registered(FrontTicket frontTicket, ActorRef sender) {
        this.frontTicket = frontTicket;
        this.sender = sender;
      }

      FrontTicket frontTicket() {
        return this.frontTicket;
      }

      public ActorRef sender() {
        return this.sender;
      }
    }

    public static Props props(ActorRef sender, ActorRef acker, EdgeId frontId) {
      return Props.create(NewFrontRegisterer.class, sender, acker, frontId);
    }

    final ActorRef sender;

    public NewFrontRegisterer(ActorRef sender, ActorRef acker, EdgeId frontId) {
      this.sender = sender;
      acker.tell(new RegisterFront(frontId), self());
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(
                      FrontTicket.class,
                      frontTicket -> {
                        context().parent().tell(new Registered(frontTicket, sender), self());
                        context().stop(self());
                      }
              )
              .build();
    }
  }

  private static class AlreadyRegisteredFrontRegisterer extends LoggingActor {
    static class Registered {
      private final FrontTicket frontTicket;
      private final @NotNull
      ActorRef sender;

      Registered(FrontTicket frontTicket, @NotNull ActorRef sender) {
        this.frontTicket = frontTicket;
        this.sender = sender;
      }

      FrontTicket frontTicket() {
        return this.frontTicket;
      }

      @NotNull
      public ActorRef sender() {
        return this.sender;
      }
    }

    public static Props props(@Nullable ActorRef sender, ActorRef acker, GlobalTime startTime) {
      return Props.create(AlreadyRegisteredFrontRegisterer.class, sender, acker, startTime);
    }

    final @Nullable
    ActorRef sender;

    public AlreadyRegisteredFrontRegisterer(@Nullable ActorRef sender, ActorRef acker, GlobalTime startTime) {
      this.sender = sender;
      acker.tell(new RegisterFrontFromTime(startTime), self());
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(
                      FrontTicket.class,
                      frontTicket -> {
                        if (sender != null) {
                          context().parent().tell(new Registered(frontTicket, sender), self());
                        }
                        context().stop(self());
                      }
              )
              .build();
    }
  }

  private final Registry registry;
  private final ActorRef acker;

  private RegistryHolder(Registry registry, ActorRef acker) {
    this.registry = registry;
    this.acker = acker;
  }

  public static Props props(Registry registry, ActorRef acker) {
    return Props.create(RegistryHolder.class, registry, acker).withDispatcher("processing-dispatcher");
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(GimmeLastCommit.class, gimmeLastCommit -> {
              log().info("Got gimme '{}'", gimmeLastCommit);
              sender().tell(new LastCommit(new GlobalTime(registry.lastCommit(), EdgeId.MIN)), self());
            })
            .match(Commit.class, commit -> {
              registry.committed(commit.globalTime().time());
              sender().tell(commit, sender());
            })
            .match(RegisterFront.class, registerFront -> registerFront(registerFront.frontId()))
            .match(NewFrontRegisterer.Registered.class, this::onNewFrontRegistered)
            .match(AlreadyRegisteredFrontRegisterer.Registered.class, this::onAlreadyRegisteredFrontRegistered)
            .build();
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    registry.all()
            .forEach((frontId, time) -> context().actorOf(AlreadyRegisteredFrontRegisterer.props(
                    null,
                    acker,
                    new GlobalTime(time, frontId)
            )));
  }

  private void registerFront(EdgeId frontId) {
    final long registeredTime = registry.registeredTime(frontId);
    if (registeredTime == -1) {
      context().actorOf(NewFrontRegisterer.props(sender(), acker, frontId));
    } else {
      context().actorOf(AlreadyRegisteredFrontRegisterer.props(
              sender(),
              acker,
              new GlobalTime(Math.max(registeredTime, registry.lastCommit()), frontId)
      ));
    }
  }

  private void onNewFrontRegistered(NewFrontRegisterer.Registered registered) {
    final GlobalTime startTime = registered.frontTicket().allowedTimestamp();
    log().info("Registering timestamp {} for {}", startTime.time(), startTime.frontId());
    registry.register(startTime.frontId(), startTime.time());
    registered.sender().tell(new FrontTicket(startTime), self());
  }

  private void onAlreadyRegisteredFrontRegistered(AlreadyRegisteredFrontRegisterer.Registered registered) {
    final GlobalTime startTime = registered.frontTicket().allowedTimestamp();
    log().info("Front '{}' has been registered already, starting from '{}'", startTime.frontId(), startTime);
    registered.sender().tell(new FrontTicket(startTime), self());
  }
}
