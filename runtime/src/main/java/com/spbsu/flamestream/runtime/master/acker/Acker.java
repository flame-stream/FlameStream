package com.spbsu.flamestream.runtime.master.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.config.AckerConfig;
import com.spbsu.flamestream.runtime.master.acker.api.Ack;
import com.spbsu.flamestream.runtime.master.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Prepared;
import com.spbsu.flamestream.runtime.master.acker.api.commit.GimmeTime;
import com.spbsu.flamestream.runtime.master.acker.api.commit.LastCommit;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Prepare;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Ready;
import com.spbsu.flamestream.runtime.master.acker.api.registry.FrontTicket;
import com.spbsu.flamestream.runtime.master.acker.api.registry.RegisterFront;
import com.spbsu.flamestream.runtime.master.acker.api.registry.UnregisterFront;
import com.spbsu.flamestream.runtime.master.acker.table.AckTable;
import com.spbsu.flamestream.runtime.master.acker.table.ArrayAckTable;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.akka.PingActor;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;


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

  private final Set<ActorRef> managers = new HashSet<>();
  private final Map<EdgeId, GlobalTime> maxHeartbeats = new HashMap<>();

  private final int managersCount;
  private final int millisBetweenCommits;
  private final AckTable table;
  private final Registry registry;
  private final ActorRef pingActor;

  private long defaultMinimalTime;
  private GlobalTime lastMinTime = GlobalTime.MIN;
  private GlobalTime lastPrepareTime = GlobalTime.MIN;
  private int committed;
  private boolean commitRuns = false;

  private Acker(int managersCount, long defaultMinimalTime, int millisBetweenCommits, Registry registry) {
    System.out.format("Acker ctr managersCount %d defaultMinimalTime %d between %d%n",
            managersCount, defaultMinimalTime, millisBetweenCommits);
    System.out.format("Acker ctr registry class %s%n", registry.getClass());
    System.out.format("Acker ctr registry %s%n", registry);
    this.managersCount = managersCount;
    this.defaultMinimalTime = defaultMinimalTime;
    this.millisBetweenCommits = millisBetweenCommits;
    this.registry = registry;

    table = new ArrayAckTable(defaultMinimalTime, SIZE, WINDOW);
    pingActor = context().actorOf(
            PingActor.props(self(), StartCommit.START),
            "acker-ping"
    );
  }

  public static Props props(int managersCount, AckerConfig ackerConfig, Registry registry) {
    return Props.create(
            Acker.class,
            managersCount,
            ackerConfig.defaultMinimalTime(),
            ackerConfig.millisBetweenCommits(),
            registry
    )
            .withDispatcher("processing-dispatcher");
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    pingActor.tell(new PingActor.Start(TimeUnit.MILLISECONDS.toNanos(millisBetweenCommits)), self());
    defaultMinimalTime = Math.max(registry.lastCommit(), defaultMinimalTime);
  }

  @Override
  public void postStop() {
    pingActor.tell(new PingActor.Stop(), self());
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(GimmeTime.class, gimmeTime -> {
              log().info("Got gimme '{}'", gimmeTime);
              //System.out.format("acker <default> got GimmeTime %s%n", gimmeTime);
              sender().tell(new LastCommit(new GlobalTime(registry.lastCommit(), EdgeId.MIN)), self());
            })
            .match(Ready.class, ready -> {
              //System.out.format("acker <default> got Ready %s%n", ready);
              //System.out.format("acker <default> got Ready managers before %s%n", managers);
              managers.add(sender());
              if (managers.size() == managersCount) {
                unstashAll();
                getContext().become(acking());
              }
            })
            .matchAny(m -> {
              System.out.format("acker <default> got something %s%n", m);
              stash();
            })
            .build();
  }

  private Receive acking() {
    return ReceiveBuilder.create()
            .match(Ack.class, this::handleAck)
            .match(Heartbeat.class, this::handleHeartBeat)
            .match(RegisterFront.class, registerFront -> registerFront(registerFront.frontId()))
            .match(UnregisterFront.class, unregisterFront -> unregisterFront(unregisterFront.frontId()))
            .match(StartCommit.class, startCommit -> commit(minAmongTables()))
            .build();
  }

  private Receive committing() {
    return acking().orElse(ReceiveBuilder.create()
            .match(Prepared.class, c -> {
              System.out.format("acker <commiting> got Prepared %s%n", c);
              committed++;
              log().info("Manager '{}' has prepared", sender());
              System.out.format("acker Prepared %d/%d%n", committed, managersCount);
              if (committed == managersCount) {
                log().info("All managers have prepared, committing");
                System.out.format("acker <commiting> all managers have prepared, committing %s", registry);

                registry.committed(lastPrepareTime.time());
                committed = 0;
                commitRuns = false;
                getContext().unbecome();
              }
            })
            .build());
  }

  private void commit(GlobalTime time) {
    //System.out.format("acker <acking> got Commit %s%n", time);
    if (!commitRuns && !time.equals(lastPrepareTime)) {
      log().info("Initiating commit for time '{}'", time);
      managers.forEach(m -> m.tell(new Prepare(time), self()));
      lastPrepareTime = time;
      commitRuns = true;
      getContext().become(committing(), false);
    }
  }

  private void registerFront(EdgeId frontId) {
    //System.out.format("acker <acking> got RegisterFront %s%n", frontId);
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
      final GlobalTime globalTime = new GlobalTime(startTime, frontId);
      maxHeartbeats.put(frontId, globalTime);
      sender().tell(new FrontTicket(globalTime), self());
    }
  }

  private void unregisterFront(EdgeId frontId) {
    //System.out.format("acker <acking> got UnregisterFront %s%n", frontId);

    log().info("Unregistering front {}", frontId);
    final GlobalTime removed = maxHeartbeats.remove(frontId);
    if (removed == null) {
      log().warning("Front " + frontId + " has been already unregistered");
    } else {
      defaultMinimalTime = Math.max(defaultMinimalTime, removed.time());
    }
    checkMinTime();
  }

  private void handleHeartBeat(Heartbeat heartbeat) {
    System.out.format("acker <acking> got HB %s%n", heartbeat);
    final GlobalTime time = heartbeat.time();
    final GlobalTime previousHeartbeat = maxHeartbeats.get(heartbeat.time().frontId());
    if (heartbeat.time().compareTo(previousHeartbeat) < 0) {
      throw new IllegalStateException("Non monotonic heartbeats");
    }
    maxHeartbeats.put(time.frontId(), heartbeat.time());
    checkMinTime();
  }

  private final Tracing.Tracer tracer = Tracing.TRACING.forEvent("ack-receive");

  private void handleAck(Ack ack) {
    tracer.log(ack.xor());
    //System.out.format("acker <acking> got Ack %s%n", ack);

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

  private enum StartCommit {
    START
  }
}
