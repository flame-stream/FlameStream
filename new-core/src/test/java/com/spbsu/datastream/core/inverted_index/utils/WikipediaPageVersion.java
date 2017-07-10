package com.spbsu.datastream.core.inverted_index.utils;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WikipediaPageVersion {
  private static final TIntIntMap versions = new TIntIntHashMap();

  public static int updateAndGetVersion(int pageId) {
    return versions.adjustOrPutValue(pageId, 1, 1);
  }
}
