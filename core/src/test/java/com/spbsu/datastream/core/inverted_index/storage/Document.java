package com.spbsu.datastream.core.inverted_index.storage;

/**
 * User: Artem
 * Date: 24.07.2017
 */
public class Document {
  private final int id;
  private final int version;

  public Document(int id, int version) {
    this.id = id;
    this.version = version;
  }

  public int id() {
    return id;
  }

  public int version() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Document document = (Document) o;
    return id == document.id;
  }

  @Override
  public int hashCode() {
    return id;
  }
}
