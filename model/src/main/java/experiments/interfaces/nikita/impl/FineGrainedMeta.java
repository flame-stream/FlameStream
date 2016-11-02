package experiments.interfaces.nikita.impl;

import experiments.interfaces.nikita.Meta;

import java.util.Comparator;

/**
 * Created by marnikitta on 02.11.16.
 */
public class FineGrainedMeta implements Meta {
    private final long major;

    private final long minor;

    private FineGrainedMeta(final long major, final long minor) {
        this.minor = minor;
        this.major = major;
    }

    public FineGrainedMeta(final long major) {
        this(major, 0);
    }

    @Override
    public FineGrainedMeta incremented() {
        return new FineGrainedMeta(this.major, this.minor + 1);
    }

    @Override
    public Comparator<Meta> comparator() {
        return Comparator.comparingLong((Meta meta) -> ((FineGrainedMeta) meta).major())
                .thenComparingLong(meta -> ((FineGrainedMeta) meta).major());
    }

    public long major() {
        return major;
    }

    public long minor() {
        return minor;
    }

    @Override
    public int compareTo(final Meta o) {
        return Comparator.comparingLong(FineGrainedMeta::major)
                .thenComparingLong(FineGrainedMeta::minor)
                .compare(this, (FineGrainedMeta) o);
    }
}
