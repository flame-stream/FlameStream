package com.spbsu.flamestream.example.bl.index.utils;

import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.util.Iterator;

/**
 * User: Artem
 * Date: 10.07.2017
 */
class WikipediaPageIterator implements Iterator<WikipediaPage> {
  private final TIntIntMap versions = new TIntIntHashMap();
  private final XMLStreamReader reader;
  private WikipediaPage next = null;

  public WikipediaPageIterator(InputStream inputStream) {
    try {
      final XMLInputFactory xmlInFact = XMLInputFactory.newInstance();
      reader = xmlInFact.createXMLStreamReader(inputStream);
    } catch (XMLStreamException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasNext() {
    try {
      while (reader.hasNext()) {
        reader.next();
        if (reader.isStartElement() && reader.getLocalName().equals("page")) {
          final int id = Integer.valueOf(reader.getAttributeValue(null, "id"));
          final String title = reader.getAttributeValue(null, "title");
          final int version = versions.adjustOrPutValue(id, 1, 1);
          next = new WikipediaPage(id, version, title, reader.getElementText());
          return true;
        }
      }
    } catch (XMLStreamException e) {
      throw new RuntimeException(e);
    }
    return false;
  }

  @Override
  public WikipediaPage next() {
    return next;
  }
}
