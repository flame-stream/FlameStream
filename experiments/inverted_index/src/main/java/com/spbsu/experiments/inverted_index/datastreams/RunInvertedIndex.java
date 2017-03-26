package com.spbsu.experiments.inverted_index.datastreams;

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
import com.spbsu.experiments.inverted_index.common_bl.actions.PageToWordPositionsFilter;
import com.spbsu.experiments.inverted_index.datastreams.actions.UpdateWordIndexFilter;
import com.spbsu.experiments.inverted_index.datastreams.actions.WordGrouping;
import com.spbsu.experiments.inverted_index.common_bl.io.WikiPageIterator;
import com.spbsu.experiments.inverted_index.common_bl.models.WikiPage;
import com.spbsu.experiments.inverted_index.common_bl.models.WordContainer;

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
                    RunInvertedIndex.class.getPackage().getName() + ".io"),
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
    final Iterator<WikiPage> wikiForiaPageIterator = new WikiPageIterator(inputStream);
    final Input iteratorInput = new IteratorInput<>(wikiForiaPageIterator, WikiPage.class);

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
    final Sink wordIndexFilter = new FilterJoba(replicator, null, new UpdateWordIndexFilter(), WordContainer[].class, WordContainer.class);
    final Sink grouping = new GroupingJoba(wordIndexFilter, types.type("<type name>"), new WordGrouping(), 2);
    final Sink pageToWordsFilter = new FilterJoba(grouping, null, new PageToWordPositionsFilter(), WordContainer.class, WordContainer.class);
    final ActorRef mergeActor = actorSystem.actorOf(ActorContainer.props(MergeActor.class, pageToWordsFilter, 2));
    final ActorSink mergeSink = new ActorSink(mergeActor);
    replicator.add(mergeSink);
    return mergeSink;
  }
}
