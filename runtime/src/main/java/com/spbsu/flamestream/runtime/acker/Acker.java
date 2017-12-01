package com.spbsu.flamestream.runtime.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.FrontTicket;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.acker.api.RegisterFront;
import com.spbsu.flamestream.runtime.acker.table.CollectiveFrontTable;
import com.spbsu.flamestream.runtime.utils.Statistics;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

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
  private static final long WINDOW = 10;
  private static final int SIZE = 100000;

  private final Set<ActorRef> minTimeSubscribers = new HashSet<>();

  private final Map<String, CollectiveFrontTable> tables = new HashMap<>();
  private final AckerStatistics stat = new AckerStatistics();
  private final AttachRegistry registry;

  private GlobalTime currentMin = GlobalTime.MIN;

  private Acker(AttachRegistry registry) {
    this.registry = registry;
  }

  public static Props props(AttachRegistry registry) {
    return Props.create(Acker.class, registry);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Ack.class, this::handleAck)
            .match(Heartbeat.class, this::handleHeartBeat)
            .match(RegisterFront.class, registerFront -> registerFront(registerFront.frontId(), registerFront.nodeId()))
            .build();
  }

  private void registerFront(String frontId, String nodeId) {
    log().info("Received front registration request for \"{}\"", frontId);
    final GlobalTime min = minAmongTables();
    log().info("Registering timestamp {} for {}", min, frontId);
    if (tables.containsKey(frontId)) {
      log().info("New front instance for frontId {}", frontId);
      tables.get(frontId).addNode(nodeId, min.time());
    } else {
      log().info("Haven't seen frontId: {}, creating new table", frontId);
      final CollectiveFrontTable frontTable = new CollectiveFrontTable(min.time(), SIZE, WINDOW);
      frontTable.addNode(nodeId, min.time());
      tables.put(frontId, frontTable);
    }
    registry.register(frontId, min.time());
    log().info("Front {} has been registered, sending ticket", frontId);
    sender().tell(new FrontTicket(frontId, nodeId, new GlobalTime(min.time(), frontId)), self());

    unstashAll();
  }

  private void handleHeartBeat(Heartbeat heartbeat) {
    final GlobalTime time = heartbeat.time();
    tables.get(time.front()).heartbeat(heartbeat.nodeId(), time.time());
    checkMinTime();
  }

  @Override
  public void postStop() {
    super.postStop();
    log().info("Acker statistics: {}", stat);
  }

  private void handleAck(Ack ack) {
    minTimeSubscribers.add(sender());
    final GlobalTime globalTime = ack.time();
    final CollectiveFrontTable table = tables.get(globalTime.front());
    final long time = globalTime.time();

    final long start = System.nanoTime();
    if (table.ack(time, ack.xor())) {
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
    if (tables.isEmpty()) {
      return GlobalTime.MIN;
    } else {
      final String[] frontMin = {"Hi"};
      final long[] timeMin = {Long.MAX_VALUE};
      tables.forEach((f, table) -> {
        final long tmpMin = table.min();
        if (tmpMin < timeMin[0] || tmpMin == timeMin[0] && f.compareTo(frontMin[0]) < 0) {
          frontMin[0] = f;
          timeMin[0] = tmpMin;
        }
      });
      return new GlobalTime(timeMin[0], frontMin[0]);
    }
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
