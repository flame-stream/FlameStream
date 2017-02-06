package com.spbsu.datastream.example.invertedindex.io;

import com.spbsu.datastream.example.invertedindex.models.WikiPage;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Author: Artem
 * Date: 17.01.2017
 */
public class WikiPageIterator implements Iterator<WikiPage> {
  private XMLStreamReader reader;
  private WikiPage next;

  public WikiPageIterator(InputStream inputStream) {
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
          int id = Integer.valueOf(reader.getAttributeValue(null, "id"));
          String title = reader.getAttributeValue(null, "title");
          next = new WikiPage(id, title, reader.getElementText());
          return true;
        }
      }
    } catch (XMLStreamException e) {
      throw new RuntimeException(e);
    }
    return false;
  }

  @Override
  public WikiPage next() {
    return next;
  }
}