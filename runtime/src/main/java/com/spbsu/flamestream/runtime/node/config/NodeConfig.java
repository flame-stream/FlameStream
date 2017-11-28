package com.spbsu.flamestream.runtime.node.config;

import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class NodeConfig {
  private final DumbInetSocketAddress address;
  private final HashRange range;

  @JsonCreator
  public NodeConfig(@JsonProperty("address") DumbInetSocketAddress address,
                    @JsonProperty("range") HashRange range) {
    this.address = address;
    this.range = range;
  }

  @JsonProperty("address")
  public DumbInetSocketAddress address() {
    return address;
  }

  @JsonProperty("range")
  public HashRange range() {
    return range;
  }

  @Override
  public String toString() {
    return "NodeConfig{" +
            "address=" + address +
            ", range=" + range +
            '}';
  }
}
