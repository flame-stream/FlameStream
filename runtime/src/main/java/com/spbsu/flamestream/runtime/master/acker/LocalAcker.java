package com.spbsu.flamestream.runtime.master.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.master.acker.api.Ack;
import com.spbsu.flamestream.runtime.master.acker.api.BufferedMessages;
import com.spbsu.flamestream.runtime.master.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.master.acker.api.NodeTime;
import com.spbsu.flamestream.runtime.master.acker.api.commit.MinTimeUpdateListener;
import com.spbsu.flamestream.runtime.master.acker.api.registry.UnregisterFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.akka.PingActor;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LocalAcker extends LoggingActor {
  private static final int FLUSH_DELAY_IN_MILLIS = 5;
  private static final int FLUSH_COUNT = 1000;

  private long nodeTime = Long.MIN_VALUE;
  private final SortedMap<GlobalTime, Long> ackCache = new TreeMap<>(Comparator.reverseOrder());
  private final Map<EdgeId, Heartbeat> lastHearbeats = new HashMap<>();
  private final List<UnregisterFront> unregisterCache = new ArrayList<>();

  private final List<ActorRef> ackers;
  private final List<ActorRef> listeners = new ArrayList<>();
  private final MinTimeUpdater minTimeUpdater;
  private final String nodeId;
  private final ActorRef pingActor;

  private int flushCounter = 0;

  public LocalAcker(List<ActorRef> ackers, String nodeId) {
    this.ackers = ackers;
    minTimeUpdater = new MinTimeUpdater(ackers);
    this.nodeId = nodeId;
    pingActor = context().actorOf(PingActor.props(self(), Flush.FLUSH));
  }

  public static Props props(List<ActorRef> ackers, String nodeId) {
    return Props.create(LocalAcker.class, ackers, nodeId).withDispatcher("processing-dispatcher");
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    minTimeUpdater.subscribe(self());
    pingActor.tell(new PingActor.Start(TimeUnit.MILLISECONDS.toNanos(FLUSH_DELAY_IN_MILLIS)), self());
  }

  @Override
  public void postStop() {
    pingActor.tell(new PingActor.Stop(), self());
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Ack.class, this::handleAck)
            .match(Flush.class, flush -> flush())
            .match(Heartbeat.class, heartbeat -> lastHearbeats.put(heartbeat.time().frontId(), heartbeat))
            .match(UnregisterFront.class, unregisterCache::add)
            .match(MinTimeUpdateListener.class, minTimeUpdateListener -> listeners.add(minTimeUpdateListener.actorRef))
            .match(MinTimeUpdate.class, minTimeUpdate -> {
              @Nullable MinTimeUpdate minTime = minTimeUpdater.onShardMinTimeUpdate(sender(), minTimeUpdate);
              if (minTime != null) {
                listeners.forEach(listener -> listener.tell(minTime, self()));
              }
            })
            .matchAny(e -> ackers.forEach(acker -> acker.forward(e, context())))
            .build();
  }

  private void tick() {
    if (flushCounter == FLUSH_COUNT) {
      flush();
    } else {
      flushCounter++;
    }
  }

  private void handleAck(Ack ack) {
    ackCache.compute(ack.time(), (globalTime, xor) -> {
      if (xor == null) {
        return ack.xor();
      } else {
        return ack.xor() ^ xor;
      }
    });

    tick();
  }

  private void flush() {
    final Map<ActorRef, List<Object>> ackerBufferedMessages = new HashMap<>();
    if (ackers.size() > 1) {
      final NodeTime nodeTime = new NodeTime(nodeId, this.nodeTime);
      ackers.forEach(acker -> ackerBufferedMessages.computeIfAbsent(acker, __ -> new ArrayList<>()).add(nodeTime));
    }
    nodeTime++;

    final boolean acksEmpty = ackCache.isEmpty();
    ackCache.entrySet()
            .stream()
            .map(entry -> new Ack(entry.getKey(), entry.getValue()))
            .collect(Collectors.groupingBy(o -> ackers.get((int) (o.time().time() % ackers.size()))))
            .forEach((acker, acks) ->
                    ackerBufferedMessages.computeIfAbsent(acker, __ -> new ArrayList<>()).addAll(acks)
            );
    ackCache.clear();

    if (!lastHearbeats.isEmpty()) {
      ackers.forEach(acker ->
              ackerBufferedMessages.computeIfAbsent(acker, __ -> new ArrayList<>()).addAll(lastHearbeats.values())
      );
      lastHearbeats.clear();
    }

    if (acksEmpty && !unregisterCache.isEmpty()) {
      ackers.forEach(acker ->
              ackerBufferedMessages.computeIfAbsent(acker, __ -> new ArrayList<>()).addAll(unregisterCache)
      );
      unregisterCache.clear();
    }

    ackerBufferedMessages.forEach((acker, messages) -> acker.tell(new BufferedMessages(messages), self()));
    flushCounter = 0;
  }

  private enum Flush {
    FLUSH
  }
}
