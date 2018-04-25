package ldbc.snb.bteronhplus.structures;

import java.util.Random;

public interface SuperNode {
    long getSize();

    long getInternalDegree();

    long getExternalDegree();

    long getId();

    Edge sampleEdge(Random random, long offset);

    long sampleNode(Random random, long offset);
}
