package com.spbsu.datastream.example.invertedindex;

import com.fasterxml.jackson.annotation.JsonProperty;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;

/**
 * Author: Artem
 * Date: 31.01.2017
 */
public class WikiPageState implements WikiPageContainer {
  @JsonProperty
  private final int pageId;
  private final TIntObjectMap<TIntSet> positionMap;

  public WikiPageState(WikiPage wikiPage, TIntObjectMap<TIntSet> positionMap) {
    pageId = wikiPage.pageId();
    this.positionMap = positionMap;
  }

  @Override
  public int pageId() {
    return pageId;
  }

  public TIntObjectMap<TIntSet> positionMap() {
    return positionMap;
  }
}
