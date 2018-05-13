package ldbc.snb.bteronhplus.structures;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class SuperNodeCluster implements SuperNode {

    private long            id;
    private List<SuperNode> children;
    private long            offsets[] = null;
    private int             size = 0;
    private long            internalDegree = 0;
    private long            externalDegree = 0;
    private double          sampleNodeExternalCumProbability[];
    private double          sampleInterNodeCumProbability[];
    private double          sampleNodeInternalCumProbability[];
    
    private int             sampleNodeExternalIndices[];
    private int             sampleInterNodeIndices[];
    private int             sampleNodeInternalIndices[];
    
    
    private double          intraNodeEdgeProb;

    public SuperNodeCluster(long id,
                            List<SuperNode> children,
                            double externalRatio
                            ) {
        this.id = id;
        this.children = children;
        this.offsets = new long[children.size()];
    
        this.sampleNodeExternalIndices = new int[children.size()];
        this.sampleInterNodeIndices = new int[children.size()];
        this.sampleNodeInternalIndices = new int[children.size()];

        offsets[0] = 0;
        for(int i = 1; i < offsets.length; ++i) {
            offsets[i] = offsets[i-1] + children.get(i-1).getSize();
        }

        long childrenInternalDegree = 0;
        for(int i = 0; i < children.size(); ++i) {
            SuperNode node = children.get(i);
            internalDegree+=node.getInternalDegree();
            externalDegree+=node.getExternalDegree();
            childrenInternalDegree+=node.getInternalDegree();
            size+=node.getSize();
        }
        long totalDegree = internalDegree+externalDegree;
        internalDegree = (long)(totalDegree*(1.0-externalRatio));
        externalDegree = (long)(totalDegree*externalRatio);
        intraNodeEdgeProb = childrenInternalDegree / (double)(internalDegree);

        long totalToExternal = 0;
        long externalEdges[] = new long[children.size()];
        long externalDegreeBudget = externalDegree;
        for(int i = 0; i < children.size() && externalDegreeBudget > 0; i++) {
            SuperNode child = children.get(i);
            long toRemove = Math.min((long)(child.getExternalDegree()*0.5),externalDegreeBudget);
           externalEdges[i] = toRemove;
           externalDegreeBudget-=toRemove;
           totalToExternal+=toRemove;
        }
        externalDegree=totalToExternal;
    
        int numExternalNonZero = 0;
        int numInterNodeNonZero = 0;
        int numInternalNonZero = 0;
        for(int i = 0; i < children.size(); ++i) {
            if(externalEdges[i] > 0) {
                sampleNodeExternalIndices[numExternalNonZero] = i;
                numExternalNonZero++;
            }
            if(children.get(i).getExternalDegree()-externalEdges[i] > 0) {
                sampleInterNodeIndices[numInterNodeNonZero] = i;
                numInterNodeNonZero++;
            }
            if(children.get(i).getInternalDegree() > 0) {
                sampleNodeInternalIndices[numInternalNonZero] = i;
                numInternalNonZero++;
            }
        }
    
        sampleNodeExternalCumProbability = new double[numExternalNonZero];
        sampleInterNodeCumProbability = new double[numInterNodeNonZero];
        sampleNodeInternalCumProbability = new double[numInternalNonZero];

        if(numExternalNonZero > 0) {
            sampleNodeExternalCumProbability[0] = 0.0;
            for (int i = 1; i < numExternalNonZero; ++i) {
                sampleNodeExternalCumProbability[i] = sampleNodeExternalCumProbability[i - 1] +
                    externalEdges[sampleNodeExternalIndices[i - 1]] /
                        (double) externalDegree;
            }
        }

        if(numInterNodeNonZero > 0) {
            sampleInterNodeCumProbability[0] = 0.0;
            for (int i = 1; i < numInterNodeNonZero; ++i) {
                SuperNode child = children.get(sampleInterNodeIndices[i - 1]);
                long childInternalDegree = child.getExternalDegree() - externalEdges[sampleInterNodeIndices[i - 1]];
                sampleInterNodeCumProbability[i] = sampleInterNodeCumProbability[i - 1] + childInternalDegree /
                    (double) (totalDegree - externalDegree - childrenInternalDegree);
            }
        }
    
        if(numInternalNonZero > 0) {
            sampleNodeInternalCumProbability[0] = 0.0;
            for (int i = 1; i < numInternalNonZero; ++i) {
                SuperNode child = children.get(sampleNodeInternalIndices[i - 1]);
                long childInternalDegree = child.getInternalDegree();
                sampleNodeInternalCumProbability[i] = sampleNodeInternalCumProbability[i - 1] +
                    childInternalDegree / (double) childrenInternalDegree;
            }
        }
    }


    @Override
    public long getSize() {
        return size;
    }

    @Override
    public long getInternalDegree() {
        return internalDegree;
    }

    @Override
    public long getExternalDegree() {
        return externalDegree;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public boolean sampleEdge(FileWriter writer, Random random, long offset) throws IOException {
        
        double prob = random.nextDouble();
        if(prob < intraNodeEdgeProb) {
    
            double prob1 = random.nextDouble();
            int pos1 = Arrays.binarySearch(sampleNodeInternalCumProbability, prob1);
            if (pos1 < 0) {
                pos1 = -(pos1 + 1);
                pos1--;
            }
    
            if (pos1 >= children.size()) {
                pos1 = children.size() - 1;
            }
    
            int index = sampleNodeInternalIndices[pos1];
            return children.get(index).sampleEdge(writer, random, offset + offsets[index]);
    
        } else {
            double prob1 = random.nextDouble();
            int pos1 = Arrays.binarySearch(sampleInterNodeCumProbability, prob1);
            if (pos1 < 0) {
                pos1 = -(pos1 + 1);
                pos1--;
            }
    
            if (pos1 >= children.size()) {
                pos1 = children.size() - 1;
            }
    
            double prob2= random.nextDouble();
            int pos2 = Arrays.binarySearch(sampleInterNodeCumProbability, prob2);
            if (pos2 < 0) {
                pos2 = -(pos2 + 1);
                pos2--;
            }
    
            if (pos2 >= children.size()) {
                pos2 = children.size() - 1;
            }
    
            int index1 = sampleInterNodeIndices[pos1];
            int index2 = sampleInterNodeIndices[pos2];
            long node1 = children.get(index1).sampleNode(random, offset + offsets[index1]);
            long node2 = children.get(index2).sampleNode(random, offset + offsets[index2]);
            writer.write(node1+"\t"+node2+"\n");
            return true;
        }

    }

    public List<SuperNode> getChildren() {
        return children;
    }

    @Override
    public long sampleNode(Random random, long offset) {

        int pos = Arrays.binarySearch(sampleNodeExternalCumProbability,random.nextDouble());

        if(pos < 0) {
            pos  = -(pos+1);
            pos--;
        }

        if(pos >= children.size()) {
            pos = children.size() - 1;
        }
        int index = sampleNodeExternalIndices[pos];
        return children.get(index).sampleNode(random, offset + offsets[index]);
    }
    
    @Override
    public void dumpInternalEdges(FileWriter writer, long offset) throws IOException {
        int index = 0;
        for(SuperNode child : children) {
            child.dumpInternalEdges(writer, offsets[index] + offset);
            index++;
        }
        
    }
}
