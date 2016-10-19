package experiments.interfaces.nikita.fake;

import experiments.interfaces.nikita.Meta;
import org.joda.time.DateTime;

/**
 * Created by marnikitta on 19.10.16.
 */
public class Timestamp implements Meta<Timestamp> {
    private final DateTime dateTime;

    private Timestamp(final DateTime dateTime) {
        this.dateTime = dateTime;
    }

    public static Timestamp now() {
        return new Timestamp(DateTime.now());
    }

    public DateTime dateTime() {
        return dateTime;
    }

    @Override
    public int compareTo(final Timestamp o) {
        return dateTime.compareTo(o.dateTime());
    }
}
