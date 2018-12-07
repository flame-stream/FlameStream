package com.spbsu.flamestream.example.bl.tfidfsd.model.entries;

import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.WordContainer;

import java.util.Objects;

public class WordEntry implements WordContainer {
    private final String word;

    public WordEntry(String word) {
        this.word = word;
    }

    @Override
    public String word() {
        return word;
    }

    @Override
    public String toString() {
        return String.format(">%s<", word);
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
