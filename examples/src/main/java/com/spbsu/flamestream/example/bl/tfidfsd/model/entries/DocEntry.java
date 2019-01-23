package com.spbsu.flamestream.example.bl.tfidfsd.model.entries;

import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;

import java.util.Objects;

public class DocEntry implements DocContainer {
    private final String document;
    private final int idfCardinality;
    private final String partitioning;

    public DocEntry(String document, int idfCardinality, String partitioning) {
        this.idfCardinality = idfCardinality;
        this.document = document;
        this.partitioning = partitioning;
    }

    @Override
    public String document() {
        return document;
    }

    @Override
    public String partitioning() {
        return partitioning;
    }

    @Override
    public int idfCardinality() {
        return idfCardinality;
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