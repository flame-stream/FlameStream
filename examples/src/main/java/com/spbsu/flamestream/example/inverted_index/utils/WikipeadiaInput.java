package com.spbsu.flamestream.example.inverted_index.utils;

import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;

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
    final Iterator<WikipediaPage> wikipediaPageIterator = new WikipediaPageIterator(inputStream);
    final Iterable<WikipediaPage> iterable = () -> wikipediaPageIterator;
    return StreamSupport.stream(iterable.spliterator(), false);
  }
}
