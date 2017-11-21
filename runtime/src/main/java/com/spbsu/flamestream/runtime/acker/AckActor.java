package com.spbsu.flamestream.runtime.acker;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.utils.Statistics;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.Commit;
import com.spbsu.flamestream.runtime.acker.api.CommitTick;
import com.spbsu.flamestream.runtime.acker.api.FrontRegistered;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.acker.api.RangeCommitDone;
import com.spbsu.flamestream.runtime.acker.api.RegisterFront;
import com.spbsu.flamestream.runtime.acker.table.AckTable;
import com.spbsu.flamestream.runtime.node.materializer.GraphRoutes;
import com.spbsu.flamestream.runtime.utils.HashRange;
import com.spbsu.flamestream.runtime.node.materializer.graph.atomic.source.api.Heartbeat;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
 * <li>{@link FrontRegistered} - reply to the front registration request. Sets the lowest allowed timestamp</li>
 * <li>{@link MinTimeUpdate} mintime</li>
 * </ol>
 * <h4>Failure Modes</h4>
 * <ol>
 * <li>{@link RuntimeException} - if something goes wrong</li>
 * </ol>
 */
public class AckActor extends LoggingActor {
  private final Map<String, AckTable> tables = new HashMap<>();
  private final AckerStatistics stat = new AckerStatistics();
  private final Collection<HashRange> committers = new HashSet<>();

  private GlobalTime currentMin = GlobalTime.MIN;

  @Nullable
  private GraphRoutes routes = null;

  private AckActor() {
  }

  public static Props props() {
    return Props.create(AckActor.class);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(GraphRoutes.class, routes -> {
              this.routes = routes;
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
            .build();
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
    log().debug("Acker tables: {}", tables);
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
      { //send min updates
        log().debug("New min time: {}", this.currentMin);
        //noinspection ConstantConditions
        tickRoutes.rangeConcierges().values().forEach(r -> r.tell(new MinTimeUpdate(this.currentMin), self()));
      }
    }

    if (minAmongTables.time() >= tickInfo.stopTs()) {
      log().info("Committing");
      tickRoutes.rangeConcierges().values().forEach(r -> r.tell(new Commit(), self()));

      getContext().become(ReceiveBuilder.create()
              .match(RangeCommitDone.class, rangeCommitDone -> {
                log().debug("Received: {}", rangeCommitDone);
                final HashRange committer = rangeCommitDone.committer();
                committers.add(committer);
                if (committers.equals(tickInfo.hashMapping().keySet())) {
                  log().info("Tick commit done");
                  tickWatcher.tell(new CommitTick(tickInfo.id()), self());
                  context().stop(self());
                }
              })
              .match(Heartbeat.class, heartbeat -> {
              })
              .build());
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
