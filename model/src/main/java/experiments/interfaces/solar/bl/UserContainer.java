package experiments.interfaces.solar.bl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import experiments.interfaces.solar.Condition;
import experiments.interfaces.solar.DataItem;

import javax.rmi.CORBA.StubDelegate;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include= JsonTypeInfo.As.WRAPPER_OBJECT, property="type")
@JsonSubTypes({
    @JsonSubTypes.Type(value=UserCounter.class, name="stat"),
    @JsonSubTypes.Type(value=UserQuery.class, name="log")
})
public interface UserContainer {
  String user();
}
