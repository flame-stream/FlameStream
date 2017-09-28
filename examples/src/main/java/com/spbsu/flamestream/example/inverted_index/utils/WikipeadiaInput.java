package com.spbsu.flamestream.example.inverted_index.utils;

import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
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
    final URL fileUrl = classLoader.getResource(dumpPath);
    if (fileUrl == null) {
      throw new RuntimeException("Dump URL is null");
    }
    final File dumpFile = new File(fileUrl.getFile());

    try {
      final InputStream inputStream = new FileInputStream(dumpFile);
      final Iterator<WikipediaPage> wikipediaPageIterator = new WikipediaPageIterator(inputStream);
      final Iterable<WikipediaPage> iterable = () -> wikipediaPageIterator;
      return StreamSupport.stream(iterable.spliterator(), false);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
