package ldbc.snb.bteronhplus.structures;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public interface SuperNode {
    long getSize();

    long getInternalDegree();

    long getExternalDegree();

    long getId();

    boolean sampleEdge(FileWriter writer, Random random, long offset) throws IOException;

    long sampleNode(Random random, long offset);
    
    void dumpInternalEdges(FileWriter writer, long offset) throws IOException;
    
}
