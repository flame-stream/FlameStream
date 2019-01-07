package com.spbsu.flamestream.runtime.master.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.config.CommitterConfig;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.master.acker.api.commit.GimmeLastCommit;
import com.spbsu.flamestream.runtime.master.acker.api.commit.LastCommit;
import com.spbsu.flamestream.runtime.master.acker.api.commit.MinTimeUpdateListener;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Prepare;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Prepared;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Ready;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.akka.PingActor;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class Committer extends LoggingActor {
  private final Set<ActorRef> managers = new HashSet<>();

  private final int managersCount;
  private final int millisBetweenCommits;
  private final Registry registry;
  private final ActorRef pingActor;

  private long minAmongTables;
  private GlobalTime lastPrepareTime = GlobalTime.MIN;
  private int committed;
  private boolean commitRuns = false;

  private Committer(int managersCount,
                    long defaultMinimalTime,
                    int millisBetweenCommits,
                    Registry registry,
                    ActorRef acker) {
    this.managersCount = managersCount;
    this.minAmongTables = defaultMinimalTime;
    this.millisBetweenCommits = millisBetweenCommits;
    this.registry = registry;

    pingActor = context().actorOf(
            PingActor.props(self(), StartCommit.START),
            "acker-ping"
    );
    acker.tell(new MinTimeUpdateListener(self()), self());
  }

  public static Props props(int managersCount, CommitterConfig committerConfig, Registry registry, ActorRef acker) {
    return Props.create(
            Committer.class,
            managersCount,
            committerConfig.defaultMinimalTime(),
            committerConfig.millisBetweenCommits(),
            registry,
            acker
    ).withDispatcher("processing-dispatcher");
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    pingActor.tell(new PingActor.Start(TimeUnit.MILLISECONDS.toNanos(millisBetweenCommits)), self());
    minAmongTables = Math.max(registry.lastCommit(), minAmongTables);
  }

  @Override
  public void postStop() {
    pingActor.tell(new PingActor.Stop(), self());
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(GimmeLastCommit.class, gimmeLastCommit -> {
              log().info("Got gimme '{}'", gimmeLastCommit);
              sender().tell(new LastCommit(new GlobalTime(registry.lastCommit(), EdgeId.MIN)), self());
            })
            .match(Ready.class, ready -> {
              managers.add(sender());
              if (managers.size() == managersCount) {
                unstashAll();
                getContext().become(waiting());
              }
            })
            .matchAny(m -> stash())
            .build();
  }

  private Receive waiting() {
    return ReceiveBuilder.create()
            .match(StartCommit.class, __ -> commit(new GlobalTime(minAmongTables, EdgeId.MIN)))
            .match(MinTimeUpdate.class, minTimeUpdate -> minAmongTables = minTimeUpdate.minTime().time())
            .build();
  }

  private Receive committing() {
    return waiting().orElse(ReceiveBuilder.create()
            .match(Prepared.class, c -> {
              committed++;
              log().info("Manager '{}' has prepared", sender());
              if (committed == managersCount) {
                log().info("All managers have prepared, committing");
                registry.committed(lastPrepareTime.time());
                committed = 0;
                commitRuns = false;
                getContext().unbecome();
              }
            })
            .build());
  }

  private void commit(GlobalTime time) {
    if (!commitRuns && !time.equals(lastPrepareTime)) {
      log().info("Initiating commit for time '{}'", time);
      managers.forEach(m -> m.tell(new Prepare(time), self()));
      lastPrepareTime = time;
      commitRuns = true;
      getContext().become(committing(), false);
    }
  }

  private enum StartCommit {
    START
  }
}
