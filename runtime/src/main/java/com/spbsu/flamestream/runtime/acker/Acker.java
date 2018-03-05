package com.spbsu.flamestream.runtime.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.acker.api.commit.Prepared;
import com.spbsu.flamestream.runtime.acker.api.commit.GimmeTime;
import com.spbsu.flamestream.runtime.acker.api.commit.LastCommit;
import com.spbsu.flamestream.runtime.acker.api.commit.Prepare;
import com.spbsu.flamestream.runtime.acker.api.commit.Ready;
import com.spbsu.flamestream.runtime.acker.api.registry.FrontTicket;
import com.spbsu.flamestream.runtime.acker.api.registry.RegisterFront;
import com.spbsu.flamestream.runtime.acker.api.registry.UnregisterFront;
import com.spbsu.flamestream.runtime.acker.table.AckTable;
import com.spbsu.flamestream.runtime.acker.table.ArrayAckTable;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * <h3>Actor Contract</h3>
 * <h4>Inbound Messages</h4>
 * <ol>
 * <li>{@link RegisterFront} requests to add frontClass to the supervision</li>
 * <li>{@link Ack} acks</li>
 * <li>{@link Heartbeat} heartbeats</li>
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
public class Acker extends LoggingActor {
  private static final int WINDOW = 1;
  private static final int SIZE = 100000;
  private static final int MIN_TIMES_TO_COMMIT = 1000000000;
  private final int managersCount;

  private final Set<ActorRef> managers = new HashSet<>();

  private final AckTable table;
  private final Map<EdgeId, GlobalTime> maxHeartbeats = new HashMap<>();
  private final Registry registry;

  private long defaultMinimalTime;
  private GlobalTime lastMinTime = GlobalTime.MIN;
  private int minTimesSinceLastCommit = 0;
  private GlobalTime lastPrepareTime = GlobalTime.MIN;
  private int committed;

  private Acker(int managersCount, long defaultMinimalTime, Registry registry) {
    this.table = new ArrayAckTable(defaultMinimalTime, SIZE, WINDOW);
    this.defaultMinimalTime = defaultMinimalTime;
    this.registry = registry;
    this.managersCount = managersCount;
  }

  public static Props props(int managersCount, long defaultMinimalTime, Registry registry) {
    return Props.create(Acker.class, managersCount, defaultMinimalTime, registry);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(GimmeTime.class, gimmeTime -> {
              log().info("Got gimme '{}'", gimmeTime);
              sender().tell(new LastCommit(new GlobalTime(registry.lastCommit(), EdgeId.MIN)), self());
            })
            .match(Ready.class, ready -> {
              managers.add(sender());
              if (managers.size() == managersCount) {
                unstashAll();
                getContext().become(acking());
              }
            })
            .matchAny(m -> stash())
            .build();
  }

  private Receive acking() {
    return ReceiveBuilder.create()
            .match(Ack.class, this::handleAck)
            .match(Heartbeat.class, this::handleHeartBeat)
            .match(RegisterFront.class, registerFront -> registerFront(registerFront.frontId()))
            .match(UnregisterFront.class, unregisterFront -> unregisterFront(unregisterFront.frontId()))
            .build();
  }

  private Receive committing() {
    return acking().orElse(ReceiveBuilder.create()
            .match(Prepared.class, c -> {
              committed++;
              log().info("Manager '{}' has prepared", sender());
              if (committed == managersCount) {
                log().info("All managers have prepared, committing");
                registry.committed(lastPrepareTime.time());
                committed = 0;
                minTimesSinceLastCommit = 0;
                getContext().unbecome();
              }
            })
            .build());
  }

  private void commit(GlobalTime time) {
    log().info("Initiating commit for time '{}'", time);
    managers.forEach(m -> m.tell(new Prepare(time), self()));
    lastPrepareTime = time;
    getContext().become(committing(), false);
  }

  private void registerFront(EdgeId frontId) {
    final long registeredTime = registry.registeredTime(frontId);
    if (registeredTime == -1) {
      final GlobalTime min = minAmongTables();

      log().info("Registering timestamp {} for {}", min, frontId);
      maxHeartbeats.put(frontId, min);
      registry.register(frontId, min.time());
      log().info("Front instance \"{}\" has been registered, sending ticket", frontId);

      sender().tell(new FrontTicket(new GlobalTime(min.time(), frontId)), self());
    } else {
      final long startTime = Math.max(registeredTime, registry.lastCommit());
      log().info("Front '{}' has been registered already, starting from '{}'", frontId, startTime);

      sender().tell(new FrontTicket(new GlobalTime(startTime, frontId)), self());
    }
  }

  private void unregisterFront(EdgeId frontId) {
    log().info("Unregistering front {}", frontId);
    defaultMinimalTime = Math.max(defaultMinimalTime, maxHeartbeats.get(frontId).time());
    maxHeartbeats.remove(frontId);
  }

  private void handleHeartBeat(Heartbeat heartbeat) {
    final GlobalTime time = heartbeat.time();
    final GlobalTime previousHeartbeat = maxHeartbeats.get(heartbeat.time().frontId());
    if (heartbeat.time().compareTo(previousHeartbeat) <= 0) {
      throw new IllegalStateException("Non monotonic heartbeats");
    }
    maxHeartbeats.put(time.frontId(), heartbeat.time());
    checkMinTime();
  }

  private final Tracing.Tracer tracer = Tracing.TRACING.forEvent("ack-receive");

  private void handleAck(Ack ack) {
    tracer.log(ack.xor());
    managers.add(sender());
    if (table.ack(ack.time().time(), ack.xor())) {
      checkMinTime();
    }
  }

  private void checkMinTime() {
    final GlobalTime minAmongTables = minAmongTables();
    if (minAmongTables.compareTo(lastMinTime) > 0) {
      this.lastMinTime = minAmongTables;
      log().debug("New min time: {}", lastMinTime);
      managers.forEach(s -> s.tell(new MinTimeUpdate(lastMinTime), self()));
      minTimesSinceLastCommit++;
      // Counter will be zeroed only after commit is done
      if (minTimesSinceLastCommit == MIN_TIMES_TO_COMMIT) {
        commit(minAmongTables);
      }
    }
  }

  private GlobalTime minAmongTables() {
    final GlobalTime minHeartbeat;
    if (maxHeartbeats.isEmpty()) {
      minHeartbeat = new GlobalTime(defaultMinimalTime, EdgeId.MIN);
    } else {
      minHeartbeat = Collections.min(maxHeartbeats.values());
    }
    final long minTime = table.tryPromote(minHeartbeat.time());
    return new GlobalTime(minTime, EdgeId.MIN);
  }
}
