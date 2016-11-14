package com.spbsu.datastream.core.io;

import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.random.FastRandom;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.item.SerializedDataItem;

import java.io.*;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class Input {

  private Stream<Stream<DataItem>> ticks(InputStream is) {
    return CharSeqTools.lines(new InputStreamReader(is, StreamTools.UTF), false)
            .map(SerializedDataItem::new)
            .map(DataItem.class::cast)
//        .map(DataItem::fromCharSeq)
            .collect(Collectors.groupingBy(di -> di.meta().tick()))
            .values().stream()
            .map(Collection::stream);
  }

  @SuppressWarnings("UnusedParameters")
  public Stream<Stream<DataItem>> stream(DataType type) {
    try {
      final FastRandom random = new FastRandom();
      final PipedInputStream pis = new PipedInputStream();
      final PipedOutputStream pos = new PipedOutputStream(pis);
      final String[] users = new String[]{"vasya", "petya", "kolya", "natasha"};
      final String[] queries = new String[100500];
      for (int i = 0; i < queries.length; i++) {
        queries[i] = random.nextLowerCaseString(10);
      }

      new Thread() {
        public void run() {
          final OutputStreamWriter writer = new OutputStreamWriter(pos);
          try {
            for (int i = 0; i < 10000; i++) {
              final int queryIndex = (int) Math.min(queries.length - 1, random.nextGamma(1, queries.length / 4));
              writer.write("{\"log\": {\"user\": \"" + users[random.nextInt(users.length)] + "\", \"query\": \"" + queries[queryIndex] + "\"}}\n");
            }
            writer.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }.start();
      return ticks(pis);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
