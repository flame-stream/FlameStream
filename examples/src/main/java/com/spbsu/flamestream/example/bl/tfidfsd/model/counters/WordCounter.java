package com.spbsu.flamestream.example.bl.tfidfsd.model.counters;

import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.WordContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.WordEntry;

public class WordCounter implements WordContainer, DocContainer {
    private final WordEntry wordEntry;
    private final int count;

    public WordCounter(WordEntry wordEntry, int count) {
        this.wordEntry = wordEntry;
        this.count = count;
    }

    @Override
    public String word() {
        return wordEntry.word();
    }

    @Override
    public String document() {
        return wordEntry.document();
    }

    public WordEntry wordEntry() {
        return wordEntry;
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

        final WordCounter that = (WordCounter) o;
        return count == that.count && wordEntry.equals(that.wordEntry);
    }

    @Override
    public int hashCode() {
        return wordEntry.hashCode();
    }

    @Override
    public String toString() {
        return String.format("<WC>%s: %d", wordEntry, count);
    }
}
