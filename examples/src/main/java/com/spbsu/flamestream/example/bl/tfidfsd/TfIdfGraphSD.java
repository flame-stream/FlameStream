package com.spbsu.flamestream.example.bl.tfidfsd;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TFObject;
import com.spbsu.flamestream.example.bl.tfidfsd.model.TextDocument;
import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.WordContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.DocCounter;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.WordCounter;
import com.spbsu.flamestream.example.bl.tfidfsd.model.counters.WordDocCounter;
import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.DocEntry;
import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.WordDocEntry;
import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.WordEntry;
import com.spbsu.flamestream.example.bl.tfidfsd.ops.entries.CountWordEntries;
import com.spbsu.flamestream.example.bl.tfidfsd.ops.entries.IDFAggregator;
import com.spbsu.flamestream.example.bl.tfidfsd.ops.filtering.DocContainerOrderingFilter;
import com.spbsu.flamestream.example.bl.tfidfsd.ops.filtering.WordContainerOrderingFilter;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TfIdfGraphSD implements Supplier<Graph> {
    private final HashFunction wordHash = HashFunction.uniformHash(HashFunction.objectHash(WordContainer.class));
    private final HashFunction docHash = HashFunction.uniformHash(
            dataItem -> dataItem.payload(DocContainer.class).document().hashCode()
    );

    @SuppressWarnings("Convert2Lambda")
    private final Equalz equalzWord = new Equalz() {
        @Override
        public boolean test(DataItem o1, DataItem o2) {
            return o1.payload(WordContainer.class).word().equals(o2.payload(WordContainer.class).word());
        }
    };

    @SuppressWarnings("Convert2Lambda")
    private final Equalz equalzDoc = new Equalz() {
        @Override
        public boolean test(DataItem o1, DataItem o2) {
            return o1.payload(DocContainer.class).document().equals(o2.payload(DocContainer.class).document());
        }
    };

    @Override
    public Graph get() {
        final Source source = new Source();
        final Grouping<WordContainer> groupingWord =
                new Grouping<>(wordHash, equalzWord, 2, WordContainer.class);

        final Grouping<DocContainer> groupingDoc =
                new Grouping<>(docHash, equalzDoc, 2, DocContainer.class);


        final FlameMap<TextDocument, WordEntry> splitterWord = new FlameMap<>(new Function<TextDocument, Stream<WordEntry>>() {
            private final Pattern pattern = Pattern.compile("\\s");

            @Override
            public Stream<WordEntry> apply(TextDocument s) {
                final Map<String, Integer> counter = new HashMap<>();
                for (String w : pattern.split(s.content())) {
                    counter.put(w, counter.getOrDefault(w, 0) + 1);
                }
                return counter.entrySet().stream()
                        .map(word -> new WordEntry(word.getKey(), s.name()));
            }
        }, TextDocument.class);

        final FlameMap<TextDocument, TFObject> splitterTF = new FlameMap<>(new Function<TextDocument, Stream<TFObject>>() {
            private final Pattern pattern = Pattern.compile("\\s");

            @Override
            public Stream<TFObject> apply(TextDocument s) {
                TFObject tfObject = new TFObject(s.name(), pattern.split(s.content()));
                return Stream.of(tfObject);
            }
        }, TextDocument.class);

        final FlameMap<List<WordContainer>, List<WordContainer>> filterWord = new FlameMap<>(
                new WordContainerOrderingFilter(),
                List.class
        );
        final FlameMap<List<WordContainer>, WordCounter> counterWord = new FlameMap<>(
                new CountWordEntries(),
                List.class
        );
        final FlameMap<List<DocContainer>, DocContainer> idfAggregator = new FlameMap<>(
                new IDFAggregator(),
                List.class
        );

        final FlameMap<List<DocContainer>, List<DocContainer>> filterDoc = new FlameMap<>(
                new DocContainerOrderingFilter(),
                List.class
        );

        final Sink sink = new Sink();
        return new Graph.Builder()
                .link(source, splitterTF)
                .link(source, splitterWord)

                .link(splitterWord, groupingWord)
                .link(groupingWord, filterWord)
                .link(filterWord, counterWord)
                .link(counterWord, groupingWord)

                .link(counterWord, groupingDoc)
                .link(splitterTF, groupingDoc)
                .link(groupingDoc, filterDoc)
                .link(filterDoc, idfAggregator)
                .link(idfAggregator, groupingDoc)

                .colocate(groupingDoc, filterDoc, idfAggregator)
                .colocate(groupingWord, filterWord, counterWord, sink)

                .link(counterWord, sink)
                .link(splitterTF, sink)

                .build(source, sink);
    }
}
