package com.spbsu.flamestream.example.bl.tfidfsd.model.counters;

import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.DocEntry;

public class DocCounter implements DocContainer {
    private final DocEntry docEntry;
    private final int count;
    private final int idfCardinality;
    private final String partitioning;

    public DocCounter(DocEntry docEntry, int count, int idfCardinality, String partitioning) {
        this.docEntry = docEntry;
        this.count = count;
        this.idfCardinality = idfCardinality;
        this.partitioning = partitioning;
    }

    @Override
    public String document() {
        return docEntry.document();
    }

    @Override
    public String partitioning() {
        return partitioning;
    }

    @Override
    public int idfCardinality() {
        return idfCardinality;
    }

    public DocEntry docEntry() {
        return docEntry;
    }

    public int count() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final DocCounter that = (DocCounter) o;
        return count == that.count && document().equals(that.document());
    }

    @Override
    public int hashCode() {
        return docEntry.document().hashCode();
    }

    @Override
    public String toString() {
        return String.format("DocCounter %s: %d", docEntry, count);
    }
}