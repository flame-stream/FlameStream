package experiments.interfaces.solar.jobas;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import com.spbsu.akka.ActorAdapter;
import com.spbsu.akka.ActorContainer;
import com.spbsu.akka.ActorMethod;
import experiments.interfaces.solar.DataItem;
import experiments.interfaces.solar.DataType;
import experiments.interfaces.solar.Joba;
import experiments.interfaces.solar.items.EndOfTick;
import experiments.interfaces.solar.items.ObjectDataItem;

import java.util.function.Function;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class FilterJoba extends Joba.Stub {
  private final Function func;
  private final Class blInput;
  private final Class blOutput;

  public FilterJoba(Joba base, DataType generates, Function func, Class blInput, Class blOutput) {
    super(generates, base);
    this.func = func;
    this.blInput = blInput;
    this.blOutput = blOutput;
  }

  @Override
  protected ActorRef actor(ActorSystem at, ActorRef sink) {
    return at.actorOf(ActorContainer.props(FilterActor.class, this, sink));
  }

  @SuppressWarnings("WeakerAccess")
  public static class FilterActor extends ActorAdapter<UntypedActor> {
    private final FilterJoba padre;
    private final ActorRef sink;

    public FilterActor(FilterJoba padre, ActorRef sink) {
      this.padre = padre;
      this.sink = sink;
    }

    @ActorMethod
    public void filter(DataItem di) {
      //noinspection unchecked
      final Object result = padre.func.apply(di.as(padre.blInput));
      if (result != null)
        sink.tell(new ObjectDataItem(result, padre.blOutput, DataItem.Meta.advance(di.meta(), padre.id())), self());
    }

    @ActorMethod
    public void kill(EndOfTick eot) {
      sink.tell(eot, sender());
    }

    @Override
    protected void postStop() {
      sink.tell(PoisonPill.getInstance(), self());
    }
  }
}
