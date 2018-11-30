package com.spbsu.flamestream.example.bl.tfidf.model.counters;

import com.spbsu.flamestream.example.bl.tfidf.model.entries.DocEntry;
import com.spbsu.flamestream.example.bl.tfidf.model.containers.DocContainer;

public class DocCounter implements DocContainer {
    private final DocEntry docEntry;
    private final int count;

    public DocCounter(DocEntry docEntry, int count) {
        this.docEntry = docEntry;
        this.count = count;
    }

    @Override
    public String document() {
        return docEntry.document();
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