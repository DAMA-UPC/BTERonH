package ldbc.snb.bteronhplus.structures;

import umontreal.iro.lecuyer.randvar.RandomVariateGen;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class RandomVariateStreamer implements SuperNodeStreamer {

    public static class RandomNode implements SuperNode {

        private int degree;
        private int id;

        RandomNode(int id, int degree) {
            this.id = id;
            this.degree = degree;
        }

        @Override
        public long getSize() {
            return 1;
        }

        @Override
        public long getInternalDegree() {
            return 0;
        }

        @Override
        public long getExternalDegree() {
            return degree;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public boolean sampleEdge(FileWriter writer, Random random, long offset) throws IOException {
            return false;
        }

        @Override
        public long sampleNode(Random random, long offset) {
            return 0;
        }
        
        @Override
        public void dumpInternalEdges(FileWriter writer, long offset) {
        
        }
    }

    RandomVariateGen randomVariateGen;
    int              nextId = 0;

    public RandomVariateStreamer(RandomVariateGen randomVariateGen) {
        this.randomVariateGen = randomVariateGen;
    }

    @Override
    public SuperNode next() {
        return new RandomNode(nextId++, (int)(double)randomVariateGen.nextDouble());
    }
}
