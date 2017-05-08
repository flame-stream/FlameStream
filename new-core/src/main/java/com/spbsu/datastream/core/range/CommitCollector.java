package com.spbsu.datastream.core.range;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.Commit;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public final class CommitCollector extends LoggingActor {
  private final Commit commit;
  private final ActorRef commitRequester;
  private final Set<ActorRef> collection;

  private final Collection<ActorRef> committed = new HashSet<>();

  private CommitCollector(final ActorRef commitRequester,
                          final Commit commit,
                          final Collection<ActorRef> collection) {
    this.commitRequester = commitRequester;
    this.commit = commit;
    this.collection = new HashSet<>(collection);
  }

  public static Props props(final ActorRef commitRequester, final Commit commit,
                            final Collection<ActorRef> collection) {
    return Props.create(CommitCollector.class, commitRequester, commit, collection);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    this.collection.forEach(ac -> ac.tell(this.commit, this.self()));
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof AtomicCommitDone) {
      this.committed.add(this.sender());
      if (this.committed.equals(this.collection)) {
        this.commitRequester.tell(new CommitsCollected(), ActorRef.noSender());
        this.context().stop(this.self());
      }
    } else {
      this.unhandled(message);
    }
  }

  public static final class AtomicCommitDone {

  }

  public static final class CommitsCollected {

  }
}
