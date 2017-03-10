package com.spbsu;

import java.net.*;

public final class AddressUtils {
  public static InetSocketAddress addressFromString(final String address) throws UnknownHostException {
    try {
      final URI uri = new URI("my://" + address);
      final String host = uri.getHost();
      final int port = uri.getPort();

      if (uri.getHost() == null || uri.getPort() == -1) {
        throw new URISyntaxException(uri.toString(),
                "URI must have host and port parts");
      } else {
        return new InetSocketAddress(InetAddress.getByName(host), port);
      }
    } catch (URISyntaxException | UnknownHostException ex) {
      throw new UnknownHostException(ex.getMessage());
    }
  }

  public static String addressToString(final InetSocketAddress address) {
    return address.getHostString() + ":" + address.getPort();
  }
}
