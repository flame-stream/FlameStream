package com.spbsu.flamestream.runtime.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class DumbInetSocketAddress {
  private final String host;
  private final int port;

  @JsonCreator
  public DumbInetSocketAddress(String address) {
    final String[] split = address.split(":");
    this.host = split[0];
    this.port = Integer.parseInt(split[1]);
  }

  @JsonCreator
  public DumbInetSocketAddress(@JsonProperty("host") String host, @JsonProperty("port") int port) {
    this.host = host;
    this.port = port;
  }

  @JsonProperty("host")
  public String host() {
    return host;
  }

  @JsonProperty("port")
  public int port() {
    return port;
  }

  @Override
  public String toString() {
    return host + ':' + port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DumbInetSocketAddress that = (DumbInetSocketAddress) o;
    return port == that.port && Objects.equals(host, that.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }
}
