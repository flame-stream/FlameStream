package com.spbsu.flamestream.runtime.node;

import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import org.apache.commons.lang.math.IntRange;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ClusterConfiguration {
  private final List<NodeConfiguration> nodeConfigs;
  private final String ackerLocation;

  @JsonCreator
  public ClusterConfiguration(@JsonProperty("nodes") List<NodeConfiguration> nodeConfigs,
                              @JsonProperty("acker_location") String ackerLocation) {
    this.nodeConfigs = new ArrayList<>(nodeConfigs);
    this.ackerLocation = ackerLocation;
  }

  @JsonProperty("nodes")
  private List<NodeConfiguration> nodeConfigsForSerialization() {
    return nodeConfigs;
  }

  @JsonProperty("acker_location")
  public String ackerLocation() {
    return ackerLocation;
  }

  @JsonIgnore
  public Stream<NodeConfiguration> nodeConfigs() {
    return nodeConfigs.stream();
  }

  @Override
  public String toString() {
    return "ClusterConfiguration{" +
            "nodeConfigs=" + nodeConfigs +
            ", ackerLocation='" + ackerLocation + '\'' +
            '}';
  }

  public static class NodeConfiguration {
    private final String id;
    private final DumbInetSocketAddress address;
    private final HashRange range;

    @JsonCreator
    public NodeConfiguration(@JsonProperty("id") String id,
                             @JsonProperty("address") DumbInetSocketAddress address,
                             @JsonProperty("range") HashRange range) {
      this.id = id;
      this.address = address;
      this.range = range;
    }

    @JsonProperty("address")
    public DumbInetSocketAddress address() {
      return address;
    }

    @JsonProperty("node_id")
    public String nodeId() {
      return id;
    }

    @JsonProperty("range")
    public HashRange range() {
      return range;
    }

    @Override
    public String toString() {
      return "NodeConfiguration{" +
              "id='" + id + '\'' +
              ", address=" + address +
              ", range=" + range +
              '}';
    }

    public static class HashRange {
      private final int from;
      private final int to;

      @JsonCreator
      public HashRange(@JsonProperty("from") int from, @JsonProperty("to") int to) {
        this.from = from;
        this.to = to;
      }

      @JsonProperty("from")
      public int from() {
        return from;
      }

      @JsonProperty("to")
      public int to() {
        return to;
      }

      public IntRange asRange() {
        return new IntRange(from, to);
      }

      @Override
      public String toString() {
        return "HashRange{" +
                "from=" + from +
                ", to=" + to +
                '}';
      }
    }
  }
}
