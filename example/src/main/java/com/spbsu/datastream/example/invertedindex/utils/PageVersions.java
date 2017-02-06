package com.spbsu.datastream.example.invertedindex.utils;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * Created by Artem on 03.02.2017.
 */
public class PageVersions {
  private static TIntIntMap versions = new TIntIntHashMap();

  public static int updateVersion(int pageId) {
    return versions.adjustOrPutValue(pageId, 1, 1);
  }
}
