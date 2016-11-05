package experiments.interfaces.solar;

import com.spbsu.commons.func.types.ConversionRepository;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.impl.TypeConvertersCollection;
import com.spbsu.commons.seq.CharSeq;
import experiments.interfaces.solar.bl.*;
import experiments.interfaces.solar.jobas.*;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class DataTypeCollection {
  public static final SerializationRepository<CharSeq> SERIALIZATION = new SerializationRepository<>(
      new TypeConvertersCollection(ConversionRepository.ROOT, UserQuery.class.getPackage().getName() + ".io"),
      CharSeq.class
  );
  private static DataTypeCollection instance;
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
      final GroupingJoba grouping = new GroupingJoba(merge, types.type("Group(Merge(UsersLog, States), UserHash, 3)"), new UserGrouping(), 2);
      final Joba states = new FilterJoba(grouping, types.type("UserCounter"), new CountUserEntries(), List.class, UserCounter.class);
      final ReplicatorJoba result = new ReplicatorJoba(states);
      merge.add(result);
      return result;
    }
    else throw new TypeUnreachableException();
  }

}
