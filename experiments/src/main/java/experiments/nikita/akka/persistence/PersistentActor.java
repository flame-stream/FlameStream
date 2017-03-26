package experiments.nikita.akka.persistence;

import akka.persistence.UntypedPersistentActor;

public class PersistentActor extends UntypedPersistentActor {
  @Override
  public String persistenceId() {
    return null;
  }

  @Override
  public void onReceiveRecover(final Object msg) throws Throwable {

  }

  @Override
  public void onReceiveCommand(final Object msg) throws Throwable {

  }
}
