package com.spbsu.datastream.example.bl.inverted_index;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class WordIndex implements WordContainer {
  @JsonProperty
  private final String word;
  @JsonProperty
  private final int count;
  @JsonProperty
  private final Queue<PageInfo> pages;

  public WordIndex(WordPage wordPage) {
    word = wordPage.word();
    count = 1;
    pages = new PriorityBlockingQueue<>();
    pages.offer(new PageInfo(wordPage.pageId(), wordPage.freq()));
  }

  public WordIndex(WordIndex wordIndex, WordPage wordPage) {
    word = wordIndex.word();
    count = wordIndex.count() + 1;
    pages = wordIndex.pages();
    pages.offer(new PageInfo(wordPage.pageId(), wordPage.freq()));
  }

  @Override
  public String word() {
    return word;
  }

  public int count() {
    return count;
  }

  public Queue<PageInfo> pages() {
    return pages;
  }
}
