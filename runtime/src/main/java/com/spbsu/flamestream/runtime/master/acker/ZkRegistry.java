package com.spbsu.flamestream.runtime.master.acker;

import com.spbsu.flamestream.core.data.meta.EdgeId;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ZkRegistry implements Registry {
  private final CuratorFramework curator;

  public ZkRegistry(CuratorFramework curator) {
    this.curator = curator;
  }

  @Override
  public Map<EdgeId, Long> all() {
    try {
      return curator.getChildren().forPath("/graph/fronts").stream().flatMap(
              edgeName ->
              {
                try {
                  return curator.getChildren()
                          .forPath("/graph/fronts/" + edgeName)
                          .stream()
                          .map(nodeId -> new EdgeId(edgeName, nodeId));
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
      ).collect(Collectors.toMap(Function.identity(), this::registeredTime));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void register(EdgeId frontId, long attachTimestamp) {
    try {
      curator.create()
              .orSetData()
              .creatingParentsIfNeeded()
              .forPath(
                      "/graph/fronts/" + frontId.edgeName() + '/' + frontId.nodeId() + "/attachTs",
                      Long.toString(attachTimestamp).getBytes()
              );
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long registeredTime(EdgeId frontId) {
    try {
      final String path = "/graph/fronts/" + frontId.edgeName() + '/' + frontId.nodeId() + "/attachTs";
      final Stat stat = curator.checkExists().forPath(path);
      if (stat != null) {
        final byte[] data = curator.getData().forPath(path);
        return Long.parseLong(new String(data));
      } else {
        return -1;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void committed(long time) {
    try {
      curator.create()
              .orSetData()
              .creatingParentsIfNeeded()
              .forPath("/graph/last-commit", Long.toString(time).getBytes());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long lastCommit() {
    try {
      final String path = "/graph/last-commit";
      final Stat stat = curator.checkExists().forPath(path);
      if (stat != null) {
        final byte[] data = curator.getData().forPath(path);
        return Long.parseLong(new String(data));
      } else {
        return 0;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
