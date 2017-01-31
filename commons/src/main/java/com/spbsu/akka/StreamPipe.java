package com.spbsu.akka;

import akka.actor.UntypedActor;
import akka.util.ByteString;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;

/**
 * User: solar
 * Date: 24.12.15
 * Time: 23:37
 */
public class StreamPipe extends ActorAdapter<UntypedActor> {
  private PipedOutputStream pipeIn;

  public StreamPipe(PipedInputStream pis) throws IOException {
    pipeIn = new PipedOutputStream(pis);
  }

  @ActorMethod
  public void invoke(ByteString string) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(string.length());
    string.copyToBuffer(buffer);
    pipeIn.write(buffer.array());
  }

  @ActorMethod
  public void invoke(Close close) throws IOException {
    pipeIn.close();
    context().stop(self());
  }

  @ActorMethod
  public void invoke(Open open) {
    sender().tell(open, self());
  }

  public static final class Close { }

  public static final class Open { }
}
