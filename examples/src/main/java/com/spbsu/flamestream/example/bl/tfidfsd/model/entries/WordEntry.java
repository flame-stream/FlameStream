package com.spbsu.flamestream.example.bl.tfidfsd.model.entries;

import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.WordContainer;

import java.util.Objects;

public class WordEntry implements WordContainer, DocContainer {
    private final String word;
    private final String docId;

    public WordEntry(String word, String docId) {
        this.word = word;
        this.docId = docId;
    }

    @Override
    public String word() {
        return word;
    }

    @Override
    public String document() {
        return docId;
    }

    @Override
    public String toString() {
        return String.format("word: >%s<, doc: >%s<", word, docId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final WordEntry wordEntry = (WordEntry) o;
        return Objects.equals(word, wordEntry.word);
    }

    @Override
    public int hashCode() {
        return Objects.hash(word);
    }
}
