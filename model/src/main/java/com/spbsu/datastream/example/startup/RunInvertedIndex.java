package com.spbsu.datastream.example.startup;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.akka.ActorContainer;
import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.MergeActor;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.StreamSink;
import com.spbsu.datastream.core.inference.DataTypeCollection;
import com.spbsu.datastream.core.io.Input;
import com.spbsu.datastream.core.io.IteratorInput;
import com.spbsu.datastream.core.job.ActorSink;
import com.spbsu.datastream.core.job.FilterJoba;
import com.spbsu.datastream.core.job.GroupingJoba;
import com.spbsu.datastream.core.job.ReplicatorJoba;
import com.spbsu.datastream.core.job.control.EndOfTick;
import com.spbsu.datastream.example.bl.inverted_index.WordContainer;
import com.spbsu.datastream.example.bl.inverted_index.WordIndex;
import com.spbsu.datastream.example.bl.inverted_index.actions.PageToWordsFilter;
import com.spbsu.datastream.example.bl.inverted_index.actions.TopKFrequentPagesFilter;
import com.spbsu.datastream.example.bl.inverted_index.actions.UpdateWordIndexFilter;
import com.spbsu.datastream.example.bl.inverted_index.actions.WordGrouping;
import com.spbsu.datastream.example.bl.inverted_index.wiki.WikiPage;
import com.spbsu.datastream.example.bl.inverted_index.wiki.WikiPageIterator;

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
    final ActorSink mergeSink = new  ActorSink(mergeActor);
    replicator.add(mergeSink);
    return mergeSink;
  }
}
