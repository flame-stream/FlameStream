package com.spbsu.flamestream.runtime.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.FrontTicket;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.acker.api.RegisterFront;
import com.spbsu.flamestream.runtime.acker.api.UnregisterFront;
import com.spbsu.flamestream.runtime.acker.table.AckTable;
import com.spbsu.flamestream.runtime.acker.table.ArrayAckTable;
import com.spbsu.flamestream.runtime.utils.Statistics;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LongSummaryStatistics;
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
  private long defaultMinimalTime;

  private final Set<ActorRef> minTimeSubscribers = new HashSet<>();

  private final AckTable table;
  private final Map<EdgeId, GlobalTime> maxHeartbeats = new HashMap<>();
  private final AckerStatistics stat = new AckerStatistics();
  private final AttachRegistry registry;

  private GlobalTime currentMin = GlobalTime.MIN;

  private Acker(long defaultMinimalTime, AttachRegistry registry) {
    this.table = new ArrayAckTable(defaultMinimalTime, SIZE, WINDOW);
    this.defaultMinimalTime = defaultMinimalTime;
    this.registry = registry;
  }

  public static Props props(long defaultMinimalTime, AttachRegistry registry) {
    return Props.create(Acker.class, defaultMinimalTime, registry);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Ack.class, this::handleAck)
            .match(Heartbeat.class, this::handleHeartBeat)
            .match(RegisterFront.class, registerFront -> registerFront(registerFront.frontId()))
            .match(UnregisterFront.class, unregisterFront -> unregisterFront(unregisterFront.frontId()))
            .build();
  }

  private void registerFront(EdgeId frontId) {
    final GlobalTime min = minAmongTables();

    log().info("Registering timestamp {} for {}", min, frontId);
    maxHeartbeats.put(frontId, min);
    registry.register(frontId, min.time());
    log().info("Front instance \"{}\" has been registered, sending ticket", frontId);

    sender().tell(new FrontTicket(new GlobalTime(min.time(), frontId)), self());
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

  @Override
  public void postStop() {
    super.postStop();
    log().info("Acker statistics: {}", stat);
  }

  //public Tracing.Tracer tracer = Tracing.TRACING.forEvent("ack-receive");
  private void handleAck(Ack ack) {
    //log().info("ACKING {}", ack);
    //tracer.log(ack.xor());
    minTimeSubscribers.add(sender());
    final long start = System.nanoTime();
    if (table.ack(ack.time().time(), ack.xor())) {
      checkMinTime();
      stat.recordReleasingAck(System.nanoTime() - start);
    } else {
      stat.recordNormalAck(System.nanoTime() - start);
    }
  }

  private void checkMinTime() {
    final GlobalTime minAmongTables = minAmongTables();
    if (minAmongTables.compareTo(currentMin) > 0) {
      this.currentMin = minAmongTables;
      log().debug("New min time: {}", currentMin);
      minTimeSubscribers.forEach(s -> s.tell(new MinTimeUpdate(currentMin), self()));
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

  private static final class AckerStatistics implements Statistics {
    private final LongSummaryStatistics normalAck = new LongSummaryStatistics();
    private final LongSummaryStatistics releasingAck = new LongSummaryStatistics();

    void recordNormalAck(long ts) {
      normalAck.accept(ts);
    }

    void recordReleasingAck(long ts) {
      releasingAck.accept(ts);
    }

    @Override
    public Map<String, Double> metrics() {
      final Map<String, Double> result = new HashMap<>();
      result.putAll(Statistics.asMap("Normal ack duration", normalAck));
      result.putAll(Statistics.asMap("Releasing ack duration", releasingAck));
      return result;
    }

    @Override
    public String toString() {
      return metrics().toString();
    }
  }
}
