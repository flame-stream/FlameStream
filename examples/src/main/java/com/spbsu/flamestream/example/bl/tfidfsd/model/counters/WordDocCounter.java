package com.spbsu.flamestream.example.bl.tfidfsd.model.counters;

import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.WordDocContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.WordDocEntry;

public class WordDocCounter implements WordDocContainer {
    private final WordDocEntry wordDocEntry;
    private final int count;
    private final String partitioning;

    public WordDocCounter(WordDocEntry wordDocEntry, int count, String partitioning) {
        this.wordDocEntry = wordDocEntry;
        this.count = count;
        this.partitioning = partitioning;
    }

    @Override
    public String  word() {
        return wordDocEntry.word();
    }

    @Override
    public String document() {
        return wordDocEntry.document();
    }

    @Override
    public String partitioning() {
        return partitioning;
    }

    @Override
    public int idfCardinality() {
        return wordDocEntry.idfCardinality();
    }

    public WordDocEntry wordDocEntry() {
        return wordDocEntry;
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

        final WordDocCounter that = (WordDocCounter) o;
        return count == that.count && wordDocEntry.equals(that.wordDocEntry);
    }

    @Override
    public int hashCode() {
        return wordDocEntry.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s: %d", wordDocEntry, count);
    }
}
