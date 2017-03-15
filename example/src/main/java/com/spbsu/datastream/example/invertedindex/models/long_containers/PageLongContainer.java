package com.spbsu.datastream.example.invertedindex.models.long_containers;

/**
 * User: Artem
 * Date: 15.03.2017
 * Time: 19:06
 */
public class PageLongContainer implements LongContainer {
  private static final int pageIdBitLength = 22;
  private static final int versionBitLength = 20;
  private static final int positionBitLength = 22;

  private final long value;

  public PageLongContainer(int pageId, int pageVersion, int position) {
    value = (((long) pageId) << (Long.SIZE - pageIdBitLength))
            | ((pageVersion & 0xffffffffL) << (Long.SIZE - pageIdBitLength - versionBitLength))
            | (position & 0xffffffffL);
  }

  public long value() {
    return value;
  }

  public int pageId() {
    return (int) (value >>> (positionBitLength + versionBitLength));
  }

  public int version() {
    return (int) ((value << pageIdBitLength) >>> (pageIdBitLength + positionBitLength));
  }

  public int position() {
    return (int) ((value << (pageIdBitLength + versionBitLength)) >>> (pageIdBitLength + versionBitLength));
  }
}
