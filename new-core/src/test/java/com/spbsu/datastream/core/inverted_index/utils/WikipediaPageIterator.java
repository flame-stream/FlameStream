package com.spbsu.datastream.core.inverted_index.utils;

import com.spbsu.datastream.core.inverted_index.model.WikipediaPage;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.util.Iterator;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WikipediaPageIterator implements Iterator<WikipediaPage> {
  private final XMLStreamReader reader;
  private WikipediaPage next;

  public WikipediaPageIterator(InputStream inputStream) {
    try {
      XMLInputFactory xmlInFact = XMLInputFactory.newInstance();
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
          next = new WikipediaPage(id, WikipediaPageVersion.updateAndGetVersion(id), title, reader.getElementText());
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
