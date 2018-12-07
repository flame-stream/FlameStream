package com.spbsu.flamestream.example.bl.tfidfsd.model.entries;

import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;

import java.util.Objects;

public class DocEntry implements DocContainer {
    private final String document;

    public DocEntry(String document) {
        this.document = document;
    }

    @Override
    public String document() {
        return document;
    }

    @Override
    public String toString() {
        return String.format("DocEntry >%s<", document);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DocEntry docEntry = (DocEntry) o;
        return Objects.equals(document, docEntry.document());
    }

    @Override
    public int hashCode() {
        return document.hashCode();
    }
}