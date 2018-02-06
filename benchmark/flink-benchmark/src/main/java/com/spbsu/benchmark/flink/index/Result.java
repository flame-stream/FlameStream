package com.spbsu.benchmark.flink.index;

import com.google.common.hash.Hashing;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.model.WordIndexRemove;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class Result {
  private final WordIndexAdd wordIndexAdd;
  private final WordIndexRemove wordIndexRemove;

  public Result(WordIndexAdd wordIndexAdd, WordIndexRemove wordIndexRemove) {
    this.wordIndexAdd = wordIndexAdd;
    this.wordIndexRemove = wordIndexRemove;
  }

  public WordIndexAdd wordIndexAdd() {
    return wordIndexAdd;
  }

  public WordIndexRemove wordIndexRemove() {
    return wordIndexRemove;
  }

  @Override
  public String toString() {
    return "Result{" + "wordIndexAdd=" + wordIndexAdd + ", wordIndexRemove=" + wordIndexRemove + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(wordIndexAdd.word(), IndexItemInLong.pageId(wordIndexAdd.positions()[0]));
  }
}
