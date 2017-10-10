package com.spbsu.flamestream.example.inverted_index.utils;

import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * User: Artem
 * Date: 21.08.2017
 */
public class WikipeadiaInput {
  public static Stream<WikipediaPage> dumpStreamFromResources(String dumpPath) {
    final ClassLoader classLoader = WikipeadiaInput.class.getClassLoader();
    final InputStream inputStream = classLoader.getResourceAsStream(dumpPath);
    return fromInputStream(inputStream);
  }

  public static Stream<WikipediaPage> dumpStreamFromFile(String filePath) {
    try {
      final InputStream inputStream = new FileInputStream(filePath);
      return fromInputStream(inputStream);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static Stream<WikipediaPage> fromInputStream(InputStream inputStream) {
    final Iterator<WikipediaPage> wikipediaPageIterator = new WikipediaPageIterator(inputStream);
    final Iterable<WikipediaPage> iterable = () -> wikipediaPageIterator;
    return StreamSupport.stream(iterable.spliterator(), false);
  }
}
