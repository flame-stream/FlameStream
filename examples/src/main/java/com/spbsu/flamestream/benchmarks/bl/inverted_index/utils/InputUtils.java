package com.spbsu.flamestream.benchmarks.bl.inverted_index.utils;

import com.spbsu.flamestream.benchmarks.bl.inverted_index.model.WikipediaPage;

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
public class InputUtils {

  public static Stream<WikipediaPage> dumpStreamFromResources(String dumpPath) throws FileNotFoundException {
    final ClassLoader classLoader = InputUtils.class.getClassLoader();
    final URL fileUrl = classLoader.getResource(dumpPath);
    if (fileUrl == null) {
      throw new RuntimeException("Dump URL is null");
    }

    final File dumpFile = new File(fileUrl.getFile());
    final InputStream inputStream = new FileInputStream(dumpFile);
    final Iterator<WikipediaPage> wikipediaPageIterator = new WikipediaPageIterator(inputStream);
    final Iterable<WikipediaPage> iterable = () -> wikipediaPageIterator;
    return StreamSupport.stream(iterable.spliterator(), false);
  }
}
