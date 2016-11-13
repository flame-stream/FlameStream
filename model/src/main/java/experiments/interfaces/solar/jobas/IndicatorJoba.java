package experiments.interfaces.solar.jobas;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import com.spbsu.akka.ActorAdapter;
import com.spbsu.akka.ActorContainer;
import com.spbsu.akka.ActorMethod;
import experiments.interfaces.solar.Condition;
import experiments.interfaces.solar.DataItem;
import experiments.interfaces.solar.Joba;
import experiments.interfaces.solar.control.ConditionFails;
import experiments.interfaces.solar.control.Control;
import experiments.interfaces.solar.control.EndOfTick;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Artem on 12.11.2016.
 */
public class IndicatorJoba extends Joba.Stub {
  private final List<Condition> conditions = new ArrayList<>();

  public IndicatorJoba(Joba base, Condition... conditions) {
    super(base.generates(), base);
    this.conditions.addAll(Arrays.asList(conditions));
  }

  @Override
  protected ActorRef actor(ActorSystem at, ActorRef sink) {
    return at.actorOf(ActorContainer.props(IndicatorActor.class, this, sink));
  }

  @SuppressWarnings("WeakerAccess")
  public static class IndicatorActor extends ActorAdapter<UntypedActor> {
    private final IndicatorJoba padre;
    private final ActorRef sink;
    private boolean stateIsOk;

    public IndicatorActor(IndicatorJoba padre, ActorRef sink) {
      this.padre = padre;
      this.sink = sink;
      stateIsOk = true;
    }

    @ActorMethod
    public void checkItem(DataItem di) {
      if (stateIsOk) {
        for (Condition c : padre.conditions) {
          //noinspection unchecked
          stateIsOk = c.update(di.as(c.getClass().getGenericSuperclass().getClass()));
        }
        if (stateIsOk) {
          sink.tell(di, self());
        }
      }
    }

    @ActorMethod
    public void control(Control eot) {
      if (eot instanceof EndOfTick) {
        if (stateIsOk) {
          sink.tell(eot, sender());
        }
        else {
          sink.tell(new ConditionFails(), sender());
        }
        context().stop(self());
      } else {
        sink.tell(eot, sender());
      }
    }
  }
}
