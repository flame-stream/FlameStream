package experiments.interfaces.solar;

import com.spbsu.commons.func.types.ConversionRepository;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.impl.TypeConvertersCollection;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.system.RuntimeUtils;
import experiments.interfaces.solar.bl.CountUserEntries;
import experiments.interfaces.solar.bl.UserCounter;
import experiments.interfaces.solar.bl.UserGrouping;
import experiments.interfaces.solar.bl.UserQuery;
import experiments.interfaces.solar.jobas.*;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class DataTypeCollection {
  public static final SerializationRepository<CharSeq> SERIALIZATION = new SerializationRepository<>(
      new TypeConvertersCollection(ConversionRepository.ROOT, UserQuery.class.getPackage().getName() + ".io"),
      CharSeq.class
  );
  private static DataTypeCollection instance = new DataTypeCollection();
  public static synchronized DataTypeCollection instance() {
    return instance;
  }

  @Nullable
  public DataType type(String name) {
      return new DataType.Stub(name);
  }

  public Joba convert(DataType from, DataType to) throws TypeUnreachableException {
    if ("UsersLog".equals(from.name()) && "Frequences".equals(to.name())) {
      final DataTypeCollection types = DataTypeCollection.instance();
      final MergeJoba merge = new MergeJoba(types.type("Merge(UsersLog, States)"), new IdentityJoba(from));
      final GroupingJoba grouping = new GroupingJoba(merge, types.type("Group(Merge(UsersLog, States), UserHash, 2)"), new UserGrouping(), 2);
      final Joba states = new FilterJoba(grouping, types.type("UserCounter"), new CountUserEntries(), RuntimeUtils.findTypeParameters(CountUserEntries.class, Function.class)[0], UserCounter.class);
      final ReplicatorJoba result = new ReplicatorJoba(states);
      merge.add(result);
      return result;
    }
    else throw new TypeUnreachableException();
  }

}
