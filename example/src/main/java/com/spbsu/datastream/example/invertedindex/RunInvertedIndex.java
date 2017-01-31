package com.spbsu.datastream.example.invertedindex;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.akka.ActorContainer;
import com.spbsu.commons.func.types.ConversionRepository;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.impl.TypeConvertersCollection;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.datastream.core.*;
import com.spbsu.datastream.core.io.Input;
import com.spbsu.datastream.core.io.IteratorInput;
import com.spbsu.datastream.core.job.FilterJoba;
import com.spbsu.datastream.core.job.GroupingJoba;
import com.spbsu.datastream.core.job.MergeActor;
import com.spbsu.datastream.core.job.ReplicatorJoba;
import com.spbsu.datastream.core.job.control.EndOfTick;
import com.spbsu.datastream.example.invertedindex.actions.PageToWordsFilter;
import com.spbsu.datastream.example.invertedindex.actions.TopKFrequentPagesFilter;
import com.spbsu.datastream.example.invertedindex.actions.UpdateWordIndexFilter;
import com.spbsu.datastream.example.invertedindex.actions.WordGrouping;
import com.spbsu.datastream.example.invertedindex.wiki.WikiPage;
import com.spbsu.datastream.example.invertedindex.wiki.WikiPageIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;

/**
 * Author: Artem
 * Date: 14.01.2017
 */
public class RunInvertedIndex {
  public static void main(String[] args) throws FileNotFoundException {
    DataStreamsContext.serializatonRepository = new SerializationRepository<>(
            new TypeConvertersCollection(ConversionRepository.ROOT,
                    WordContainer.class.getPackage().getName() + ".io"),
            CharSeq.class
    );
    final ActorSystem akka = ActorSystem.create();
    final DataTypeCollection types = (DataTypeCollection) DataStreamsContext.typeCollection;

    final ClassLoader classLoader = RunInvertedIndex.class.getClassLoader();
    final URL fileUrl = classLoader.getResource("wikipedia/small_dump_example.xml");
    if (fileUrl == null) {
      throw new RuntimeException("Dump URL is null");
    }

    final File dumpFile = new File(fileUrl.getFile());
    final InputStream inputStream = new FileInputStream(dumpFile);
    final Iterator<WikiPage> wikiPageIterator = new WikiPageIterator(inputStream);
    final Input iteratorInput = new IteratorInput<>(wikiPageIterator, WikiPage.class);

    iteratorInput.stream(null).flatMap((input) -> {
      final StreamSink sink = new StreamSink();
      final Sink joba = makeJoba(akka, sink, types);
      new Thread(() -> {
        input.forEach(joba::accept);
        joba.accept(new EndOfTick());
      }).start();
      return sink.stream().onClose(DataStreamsContext.output::commit);
    }).forEach(DataStreamsContext.output.processor());

    akka.shutdown();
  }

  private static Sink makeJoba(ActorSystem actorSystem, Sink sink, DataTypeCollection types) {
    final ReplicatorJoba replicator = new ReplicatorJoba(sink);
    final Sink topKFilter = new FilterJoba(replicator, null, new TopKFrequentPagesFilter(5), WordIndex.class, WordIndex.class);
    final Sink wordIndexFilter = new FilterJoba(topKFilter, null, new UpdateWordIndexFilter(), WordContainer[].class, WordIndex.class);
    final Sink grouping = new GroupingJoba(wordIndexFilter, types.type("<type name>"), new WordGrouping(), 2);
    final Sink pageToWordsFilter = new FilterJoba(grouping, null, new PageToWordsFilter(), WordContainer.class, WordContainer.class);
    final ActorRef mergeActor = actorSystem.actorOf(ActorContainer.props(MergeActor.class, pageToWordsFilter, 2));
    final ActorSink mergeSink = new ActorSink(mergeActor);
    replicator.add(mergeSink);
    return mergeSink;
  }
}
