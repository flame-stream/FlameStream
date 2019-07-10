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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;


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

    public static Props props(ActorRef sender, List<ActorRef> ackers, EdgeId frontId, long defaultMinimalTime) {
      return Props.create(NewFrontRegisterer.class, sender, ackers, frontId, defaultMinimalTime);
    }

    final ActorRef sender;
    final Set<ActorRef> ackersWaitedFor;
    GlobalTime maxAllowedTimestamp;

    public NewFrontRegisterer(ActorRef sender, List<ActorRef> ackers, EdgeId frontId, long defaultMinimalTime) {
      this.sender = sender;
      ackersWaitedFor = new LinkedHashSet<>(ackers);
      maxAllowedTimestamp = new GlobalTime(defaultMinimalTime, frontId);
      ackers.forEach(acker -> acker.tell(new RegisterFront(frontId), self()));
      checkIfRegistered();
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(
                      FrontTicket.class,
                      frontTicket -> {
                        if (maxAllowedTimestamp.compareTo(frontTicket.allowedTimestamp()) < 0) {
                          maxAllowedTimestamp = frontTicket.allowedTimestamp();
                        }
                        ackersWaitedFor.remove(sender());
                        checkIfRegistered();
                      }
              )
              .build();
    }

    private void checkIfRegistered() {
      if (ackersWaitedFor.isEmpty()) {
        context().parent().tell(new Registered(new FrontTicket(maxAllowedTimestamp), sender), self());
        context().stop(self());
      }
    }
  }

  private static class AlreadyRegisteredFrontRegisterer extends LoggingActor {
    private final GlobalTime startTime;

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

    public static Props props(@Nullable ActorRef sender, List<ActorRef> ackers, GlobalTime startTime) {
      return Props.create(AlreadyRegisteredFrontRegisterer.class, sender, ackers, startTime);
    }

    final @Nullable
    ActorRef sender;
    final Set<ActorRef> ackersWaitedFor;

    public AlreadyRegisteredFrontRegisterer(@Nullable ActorRef sender, List<ActorRef> ackers, GlobalTime startTime) {
      this.sender = sender;
      this.startTime = startTime;
      ackersWaitedFor = new LinkedHashSet<>(ackers);
      ackers.forEach(acker -> acker.tell(new RegisterFrontFromTime(startTime), self()));
      checkIfRegistered();
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(FrontTicket.class, ignored -> {
                ackersWaitedFor.remove(sender());
                checkIfRegistered();
              })
              .build();
    }

    private void checkIfRegistered() {
      if (ackersWaitedFor.isEmpty()) {
        if (sender != null) {
          context().parent().tell(new Registered(new FrontTicket(startTime), sender), self());
        }
        context().stop(self());
      }
    }
  }

  private final Registry registry;
  private final List<ActorRef> ackers;
  private final long defaultMinimalTime;

  private RegistryHolder(Registry registry, List<ActorRef> ackers, long defaultMinimalTime) {
    this.registry = registry;
    this.ackers = ackers;
    this.defaultMinimalTime = defaultMinimalTime;
  }

  public static Props props(Registry registry, List<ActorRef> ackers, long defaultMinimalTime) {
    return Props.create(RegistryHolder.class, registry, ackers, defaultMinimalTime)
            .withDispatcher("processing-dispatcher");
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(GimmeLastCommit.class, gimmeLastCommit -> {
              log().info("Got gimme '{}'", gimmeLastCommit);
              sender().tell(new LastCommit(new GlobalTime(registry.lastCommit(), EdgeId.Min.INSTANCE)), self());
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
    registry.registeredFronts()
            .forEach((frontId, time) -> context().actorOf(AlreadyRegisteredFrontRegisterer.props(
                    null,
                    ackers,
                    new GlobalTime(Math.max(time, registry.lastCommit()), frontId)
            )));
  }

  private void registerFront(EdgeId frontId) {
    final long registeredTime = registry.registeredTime(frontId);
    if (registeredTime == -1) {
      context().actorOf(NewFrontRegisterer.props(sender(), ackers, frontId, defaultMinimalTime));
    } else {
      context().actorOf(AlreadyRegisteredFrontRegisterer.props(
              sender(),
              ackers,
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
