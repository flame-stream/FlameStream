package com.spbsu.datastream.example.invertedindex;

import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Artem
 * Date: 01.02.2017
 */
public class WordDictionary {
  private final static TObjectIntMap<String> wordToId = new TObjectIntHashMap<>();
  private final static List<String> idToWord = new ArrayList<>();

  public static int getWordId(String word) {
    if (wordToId.get(word) == Constants.DEFAULT_INT_NO_ENTRY_VALUE) {
      idToWord.add(word);
      wordToId.put(word, idToWord.size());
      return idToWord.size();
    } else {
      return wordToId.get(word);
    }
  }

  public static String getWordById(int id) {
    return idToWord.get(id - 1);
  }
}