package com.spbsu.flamestream.runtime.config;

import akka.actor.ActorPath;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.flamestream.runtime.serialization.FlameSerializer;
import com.spbsu.flamestream.runtime.serialization.JacksonSerializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.CreateMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ZookeeperWorkersNode {
  private final CuratorFramework curator;
  private final String path;
  private final FlameSerializer jacksonSerializer = new JacksonSerializer();
  private final PathChildrenCache pathChildrenCache;

  static public class Worker {
    public final String id;

    @JsonProperty
    public String id() { return id;}

    final ActorPath actorPath;

    @JsonProperty
    ActorPath actorPath() { return actorPath; }

    @JsonCreator
    Worker(@JsonProperty("id") String id, @JsonProperty("actorPath") ActorPath actorPath) {
      this.id = id;
      this.actorPath = actorPath;
    }
  }

  public ZookeeperWorkersNode(CuratorFramework curator, String path) throws Exception {
    this.curator = curator;
    this.path = path;
    this.pathChildrenCache = new PathChildrenCache(curator, path, true);
    pathChildrenCache.start();
    pathChildrenCache.rebuild();
  }

  public void create(String id, ActorPath actorPath) throws Exception {
    curator.create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
            .forPath(path + "/_", jacksonSerializer.serialize(new Worker(id, actorPath)));
    pathChildrenCache.rebuild();
  }

  public List<Worker> stream() {
    return pathChildrenCache.getCurrentData()
            .stream()
            .map(worker -> jacksonSerializer.deserialize(worker.getData(), Worker.class)).collect(Collectors.toList());
  }

  public ClusterConfig clusterConfig() {
    final Map<String, ActorPath> paths = stream().stream().collect(Collectors.toMap(Worker::id, Worker::actorPath));
    final Map<String, HashGroup> ranges = new HashMap<>();
    final List<HashUnit> covering = HashUnit.covering(paths.size() - 1)
            .collect(Collectors.toCollection(ArrayList::new));
    final String masterLocation = stream().get(0).id;
    paths.keySet().forEach(s -> {
      if (s.equals(masterLocation)) {
        ranges.put(s, new HashGroup(Collections.singleton(new HashUnit(0, 0))));
      } else {
        ranges.put(s, new HashGroup(Collections.singleton(covering.get(0))));
        covering.remove(0);
      }
    });
    assert covering.isEmpty();

    return new ClusterConfig(paths, masterLocation, ranges);
  }
}
