package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.edge.Front;
import com.spbsu.flamestream.runtime.edge.Rear;
import com.spbsu.flamestream.runtime.edge.SystemEdgeContext;
import com.spbsu.flamestream.runtime.serialization.FlameSerializer;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

public class RemoteRuntime implements FlameRuntime {
  private final Logger log = LoggerFactory.getLogger(RemoteRuntime.class);
  private final FlameSerializer serializer;
  private final ClusterConfig config;
  private final CuratorFramework curator;

  public RemoteRuntime(CuratorFramework curator, FlameSerializer serializer, ClusterConfig config) {
    this.curator = curator;
    this.serializer = serializer;
    this.config = config;
  }

  @Override
  public Flame run(Graph g) {
    log.info("Pushing graph {}", g);
    try {
      curator.create().orSetData().forPath("/graph", serializer.serialize(g));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new RemoteFlame(config);
  }

  private class RemoteFlame implements Flame {
    private final ClusterConfig clusterConfig;

    private RemoteFlame(ClusterConfig config) {
      this.clusterConfig = config;
    }

    @Override
    public void close() {
      log.info("Extinguishing graph");
    }

    @Override
    public <F extends Front, H> Stream<H> attachFront(String id, FrontType<F, H> type) {
      log.info("Attaching front id: '{}', type: '{}'", id, type);
      try {
        curator.create()
                .orSetData()
                .creatingParentsIfNeeded()
                .forPath("/graph/fronts/" + id, serializer.serialize(type.instance()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return clusterConfig.paths()
              .entrySet()
              .stream()
              .map(e -> type.handle(new SystemEdgeContext(e.getValue(), e.getKey(), id)));
    }

    @Override
    public <R extends Rear, H> Stream<H> attachRear(String id, RearType<R, H> type) {
      log.info("Attaching rear id: '{}', type: '{}'", id, type);
      try {
        curator.create()
                .orSetData()
                .creatingParentsIfNeeded()
                .forPath("/graph/rears/" + id, serializer.serialize(type.instance()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return clusterConfig.paths()
              .entrySet()
              .stream()
              .map(e -> type.handle(new SystemEdgeContext(e.getValue(), e.getKey(), id)));
    }
  }
}
