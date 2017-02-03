package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.datastream.example.invertedindex.WikiPageContainer;
import com.spbsu.datastream.example.invertedindex.WordOutput;

import java.util.function.Function;

/**
 * Author: Artem
 * Date: 02.02.2017
 */
public class WordOutputFilter implements Function<WikiPageContainer, WikiPageContainer> {
  @Override
  public WikiPageContainer apply(WikiPageContainer wikiPageContainer) {
    if (wikiPageContainer instanceof WordOutput) {
      return null;
    }
    return wikiPageContainer;
  }
}
