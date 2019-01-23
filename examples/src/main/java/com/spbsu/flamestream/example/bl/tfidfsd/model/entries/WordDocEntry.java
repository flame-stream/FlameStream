package com.spbsu.flamestream.example.bl.tfidfsd.model.entries;

import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.WordDocContainer;

import java.util.Objects;

public class WordDocEntry implements WordDocContainer {
    private final String word;
    private final String document;
    private final String partitioning;
    private final int idfCardinality;

    public WordDocEntry(String document, String word, int idfCardinality, String partitioning) {
        this.document = document;
        this.word = word;
        this.idfCardinality = idfCardinality;
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
    public String word() {
        return word;
    }

    @Override
    public String toString() {
        return String.format("%s: >%s<", document, word);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final WordDocEntry wordEntry = (WordDocEntry) o;
        return Objects.equals(word, wordEntry.word) && Objects.equals(document, wordEntry.document());
    }

    @Override
    public int hashCode() {
        return Objects.hash(word, document);
    }
}
