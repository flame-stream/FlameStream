package com.spbsu.flamestream.example.bl.tfidf;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.example.bl.tfidf.model.TextDocument;
import com.spbsu.flamestream.example.bl.tfidf.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.containers.WordContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.containers.WordDocContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.DocCounter;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.WordCounter;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.WordDocCounter;
import com.spbsu.flamestream.example.bl.tfidf.model.entries.DocEntry;
import com.spbsu.flamestream.example.bl.tfidf.model.entries.WordDocEntry;
import com.spbsu.flamestream.example.bl.tfidf.ops.entries.CountDocEntries;
import com.spbsu.flamestream.example.bl.tfidf.ops.entries.CountWordDocEntries;
import com.spbsu.flamestream.example.bl.tfidf.ops.entries.CountWordDocSingle;
import com.spbsu.flamestream.example.bl.tfidf.ops.entries.CountWordEntries;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.DocContainerOrderingFilter;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.WordContainerOrderingFilter;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.WordDoc2WordFilter;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.WordDocContainerOrderingFilter;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class TfIdfGraph implements Supplier<Graph> {
    private final HashFunction wordHash = HashFunction.uniformHash(HashFunction.objectHash(WordContainer.class));
    private final HashFunction docHash = HashFunction.uniformHash(HashFunction.objectHash(DocContainer.class));
    private final HashFunction wordDocHash = HashFunction.uniformHash(HashFunction.objectHash(WordDocContainer.class));

    @SuppressWarnings("Convert2Lambda")
    private final Equalz equalz = new Equalz() {
        @Override
        public boolean test(DataItem o1, DataItem o2) {
            return o1.payload(WordContainer.class).word().equals(o2.payload(WordContainer.class).word());
        }
    };

    @SuppressWarnings("Convert2Lambda")
    private final Equalz equalzWordDoc = new Equalz() {
        @Override
        public boolean test(DataItem o1, DataItem o2) {
            return o1.payload(WordDocContainer.class).word().equals(o2.payload(WordDocContainer.class).word());
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

        final Grouping<WordDocContainer> groupingWordDoc =
                new Grouping<>(wordDocHash, equalzWordDoc, 2, WordDocContainer.class);
        final Grouping<DocContainer> groupingDoc =
                new Grouping<>(docHash, equalzDoc, 2, DocContainer.class);
        final Grouping<WordDocContainer> groupingWordDocSingle =
                new Grouping<>(wordDocHash, equalz, 2, WordDocContainer.class);
        final Grouping<WordContainer> groupingIdf = new Grouping<>(wordHash, equalz, 2, WordContainer.class);

        final FlameMap<TextDocument, WordDocEntry> splitterWordDoc = new FlameMap<>(new Function<TextDocument, Stream<WordDocEntry>>() {
            private final Pattern pattern = Pattern.compile("\\s");

            @Override
            public Stream<WordDocEntry> apply(TextDocument s) {
                return Arrays.stream(pattern.split(s.content()))
                        .map(word -> new WordDocEntry(s.name(), word));
            }
        }, TextDocument.class);

        final FlameMap<TextDocument, DocEntry> splitterDoc = new FlameMap<>(new Function<TextDocument, Stream<DocEntry>>() {
            private final Pattern pattern = Pattern.compile("\\s");

            @Override
            public Stream<DocEntry> apply(TextDocument s) {
                return Arrays.stream(pattern.split(s.content())).map(word -> new DocEntry(s.name()));
            }
        }, TextDocument.class);


        final FlameMap<List<WordDocContainer>, List<WordDocContainer>> filterWordDoc = new FlameMap<>(
                new WordDocContainerOrderingFilter(),
                List.class
        );
        final FlameMap<List<DocContainer>, List<DocContainer>> filterDoc = new FlameMap<>(
                new DocContainerOrderingFilter(),
                List.class
        );
        final FlameMap<List<WordDocContainer>, List<WordDocContainer>> filterWordDocSingle = new FlameMap<>(
                new WordDocContainerOrderingFilter(),
                List.class
        );
        final FlameMap<WordDocCounter, WordContainer> filterWordDoc2Doc = new FlameMap<>(
                new WordDoc2WordFilter(),
                WordDocCounter.class
        );
        final FlameMap<List<WordContainer>, List<WordContainer>> filterIdf = new FlameMap<>(
                new WordContainerOrderingFilter(),
                List.class
        );

        final FlameMap<List<WordDocContainer>, WordDocCounter> counterWordDoc = new FlameMap<>(
                new CountWordDocEntries(),
                List.class
        );
        final FlameMap<List<DocContainer>, DocCounter> counterDoc = new FlameMap<>(
                new CountDocEntries(),
                List.class
        );
        final FlameMap<List<WordDocContainer>, WordDocCounter> counterWordDocSingle = new FlameMap<>(
                new CountWordDocSingle(),
                List.class
        );
        final FlameMap<List<WordContainer>, WordCounter> counterIdf = new FlameMap<>(
                new CountWordEntries(),
                List.class
        );

        final Sink sink = new Sink();
        return new Graph.Builder()
                .link(source, splitterWordDoc)
                .link(source, splitterDoc)

                .link(splitterWordDoc, groupingWordDoc)
                .link(splitterDoc, groupingDoc)
                .link(splitterWordDoc, groupingWordDocSingle)

                .link(groupingWordDoc, filterWordDoc)
                .link(groupingWordDocSingle, filterWordDocSingle)
                .link(groupingDoc, filterDoc)

                .link(filterWordDoc, counterWordDoc)
                .link(filterDoc, counterDoc)
                .link(filterWordDocSingle, counterWordDocSingle)

                .link(counterWordDoc, sink)
                .link(counterDoc, sink)
                .link(counterWordDocSingle, filterWordDoc2Doc)

                .link(counterWordDoc, groupingWordDoc)
                .link(counterDoc, groupingDoc)
                .link(counterWordDocSingle, groupingWordDocSingle)

                .link(filterWordDoc2Doc, groupingIdf)
                .link(groupingIdf, filterIdf)
                .link(filterIdf, counterIdf)
                .link(counterIdf, groupingIdf)
                .link(counterIdf, sink)

                .colocate(source, splitterWordDoc)
                .colocate(groupingWordDoc, filterWordDoc, counterWordDoc, sink)
                .build(source, sink);
    }
}
