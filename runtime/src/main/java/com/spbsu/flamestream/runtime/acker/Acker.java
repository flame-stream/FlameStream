package com.spbsu.flamestream.runtime.acker;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.graph.FlameRouter;
import com.spbsu.flamestream.runtime.utils.Statistics;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.FrontTicket;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.acker.api.RegisterFront;
import com.spbsu.flamestream.runtime.acker.table.AckTable;
import com.spbsu.flamestream.runtime.acker.table.ArrayAckTable;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;


/**
 * <h3>Actor Contract</h3>
 * <h4>Inbound Messages</h4>
 * <ol>
 * <li>{@link RegisterFront} requests to add front to the supervision</li>
 * <li>{@link Ack} acks</li>
 * <li>{@link Heartbeat} heartbeats</li>
 * </ol>
 * <h4>Outbound Messages</h4>
 * <ol>
 * <li>{@link FrontTicket} - reply to the front registration request. Sets the lowest allowed timestamp</li>
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

  private final Map<String, AckTable> tables = new HashMap<>();
  private final AckerStatistics stat = new AckerStatistics();
  private final AttachRegistry registry;
  private FlameRouter router = null;

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
            .match(FlameRouter.class, routes -> {
              this.router = routes;
              unstashAll();
              getContext().become(acking());
            })
            .matchAny(m -> stash())
            .build();
  }

  private Receive acking() {
    return ReceiveBuilder.create()
            .match(Ack.class, this::handleAck)
            .match(Heartbeat.class, this::handleHeartBeat)
            .match(RegisterFront.class, registerFront -> handleRegister(registerFront.id()))
            .build();
  }

  private void handleRegister(String frontId) {
    final GlobalTime min = minAmongTables();
    tables.put(frontId, new ArrayAckTable(min.time(), SIZE, WINDOW));
    registry.register(frontId, min.time());
    sender().tell(new FrontTicket(frontId, new GlobalTime(min.time(), frontId)), self());
  }

  private void handleHeartBeat(Heartbeat heartbeat) {
    final GlobalTime time = heartbeat.time();
    tables.get(time.front()).heartbeat(time.time());
    checkMinTime();
  }

  @Override
  public void postStop() {
    super.postStop();
    log().info("Acker statistics: {}", stat);
  }

  private void handleAck(Ack ack) {
    final GlobalTime globalTime = ack.time();
    final AckTable ackTable = tables.get(globalTime.front());
    final long time = globalTime.time();

    final long start = System.nanoTime();
    if (ackTable.ack(time, ack.xor())) {
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
      router.broadcast(new MinTimeUpdate(currentMin), self());
    }
  }

  private GlobalTime minAmongTables() {
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
