package com.spbsu.flamestream.runtime.zk;

import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.Job;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.function.Consumer;
import java.util.stream.Stream;

public class ZooKeeperExternalClient extends ZooKeeperBaseClient {

  public ZooKeeperExternalClient(ZooKeeper zooKeeper) {
    super(zooKeeper);
  }

  /**
   * Watcher is called when new job appears
   */
  public void watchJob(Consumer<Job> watcher) {
    try {
      final Stat exists = zooKeeper.exists("/job/graph", event -> {
        if (event.getType() == Watcher.Event.EventType.NodeCreated) {
          watcher.accept(retrieveJob());
        }
      });
      if (exists != null) {
        watcher.accept(retrieveJob());
      }
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private Job retrieveJob() {
    try {
      final byte[] graphData = zooKeeper.getData(
              "/job/graph",
              false,
              null
      );
      final ByteBufferInput input = new ByteBufferInput(graphData);
      final Graph graph = (Graph) kryo.readClassAndObject(input);
      final Stream<Job.Front> fronts = zooKeeper.getChildren("/job/fronts", event -> {}, null)
              .stream()
              .map(this::frontBy);
      final Stream<Job.Rear> rears = zooKeeper.getChildren("/job/rears", event -> {}, null)
              .stream()
              .map(this::rearBy);
      return new Job() {
        @Override
        public Graph graph() {
          return graph;
        }

        @Override
        public Stream<Front> fronts() {
          return fronts;
        }

        @Override
        public Stream<Rear> rears() {
          return rears;
        }
      };
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private Job.Front frontBy(String name) {
    try {
      final byte[] data = zooKeeper.getData("/job/fronts/" + name, false, null);
      final ByteBufferInput input = new ByteBufferInput(data);
      return (Job.Front) kryo.readClassAndObject(input);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private Job.Rear rearBy(String name) {
    try {
      final byte[] data = zooKeeper.getData("/job/rears/" + name, false, null);
      final ByteBufferInput input = new ByteBufferInput(data);
      return (Job.Rear) kryo.readClassAndObject(input);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
