package com.spbsu.datastream.example.invertedindex.utils;

/**
 * Created by Artem on 03.02.2017.
 */
public class PagePositionLong {
  private static final int pageIdBitLength = 24;
  private static final int positionBitLength = 20;
  private static final int versionBitLength = 20;

  public static long createPagePosition(int pageId, int position, int pageVersion) {
    return (((long) pageId) << (Long.SIZE - pageIdBitLength))
            | ((position & 0xffffffffL) << (Long.SIZE - pageIdBitLength - positionBitLength))
            | ((pageVersion & 0xffffffffL));
  }

  public static int pageId(long l) {
    return (int) (l >> (positionBitLength + versionBitLength));
  }

  public static int position(long l) {
    return (int) ((l << pageIdBitLength) >> (pageIdBitLength + positionBitLength));
  }

  public static int version(long l) {
    return (int) ((l << (pageIdBitLength + positionBitLength)) >> (pageIdBitLength + positionBitLength));
  }
}
