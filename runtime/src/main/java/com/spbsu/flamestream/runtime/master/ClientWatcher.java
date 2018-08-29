package com.spbsu.flamestream.runtime.master;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Job;
import com.spbsu.flamestream.runtime.RemoteRuntime;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.edge.socket.SocketFrontType;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;
import com.spbsu.flamestream.runtime.serialization.FlameSerializer;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClientWatcher extends LoggingActor {
  private final CuratorFramework curator;
  private final FlameSerializer serializer;
  private final ClusterConfig config;

  private PathChildrenCache jobsCache = null;
  private RemoteRuntime remoteRuntime = null;

  private ClientWatcher(CuratorFramework curator, FlameSerializer serializer, ClusterConfig config) {
    this.curator = curator;
    this.serializer = serializer;
    this.config = config;
  }

  public static Props props(CuratorFramework curator, FlameSerializer serializer, ClusterConfig config) {
    return Props.create(ClientWatcher.class, curator, serializer, config);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    remoteRuntime = new RemoteRuntime(curator, serializer, config);

    jobsCache = new PathChildrenCache(curator, "/jobs", false);
    final boolean[] init = {false};
    final boolean[] jobAdded = {false};
    jobsCache.getListenable().addListener((curatorFramework, event) -> {
      if (event.getType() == PathChildrenCacheEvent.Type.INITIALIZED) {
        final List<ChildData> initialJobs = new ArrayList<>(event.getInitialData());
        if (!initialJobs.isEmpty()) {
          Collections.sort(initialJobs);
          final Job job = serializer.deserialize(curator.getData()
                  .forPath(initialJobs.get(initialJobs.size() - 1).getPath()), Job.class);
          onNewJob(job);
          jobAdded[0] = true;
        }
        init[0] = true;
      } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED && init[0]) {
        // TODO: 17.07.18 implement job updating
        if (jobAdded[0]) {
          throw new RuntimeException("Updating job is not implemented yet");
        }
        final Job job = serializer.deserialize(curator.getData().forPath(event.getData().getPath()), Job.class);
        onNewJob(job);
        jobAdded[0] = true;
      }
    });
    jobsCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
  }

  @Override
  public void postStop() {
    try {
      jobsCache.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      curator.close();
    }
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create().build();
  }

  private void onNewJob(Job job) {
    final RemoteRuntime.Flame flame = remoteRuntime.run(job.graph());
    job.fronts()
            .forEach(front -> flame.attachFront(
                    front.id(),
                    new SocketFrontType(front.host(), front.port(), front.inputClasses().toArray(Class[]::new))
            ));
    job.rears()
            .forEach(rear -> flame.attachRear(
                    rear.id(),
                    new SocketRearType(rear.host(), rear.port(), rear.outputClasses().toArray(Class[]::new))
            ));
  }
}
