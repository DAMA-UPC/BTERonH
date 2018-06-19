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
    private double          sampleNodeInternalProbability[];
    private double          sampleInterNodeProbability[];
    private long            interDegree[];
    
    private int             sampleNodeExternalIndices[];
    
    
    private double          intraNodeEdgeProb;

    public SuperNodeCluster(long id,
                            List<SuperNode> children,
                            double externalRatio
                            ) {
        this.id = id;
        this.children = new ArrayList<SuperNode>(children);
        this.children.sort( new Comparator<SuperNode>() {
            @Override
            public int compare(SuperNode superNode1, SuperNode superNode2) {
                return -1*(int)(superNode1.getExternalDegree()-superNode2.getExternalDegree());
            }
        });
        
        this.offsets = new long[children.size()];
    
        this.sampleNodeExternalIndices = new int[children.size()];

        offsets[0] = 0;
        for(int i = 1; i < offsets.length; ++i) {
            offsets[i] = offsets[i-1] + children.get(i-1).getSize();
        }

        long tempInternalDegree = 0;
        long tempExternalDegree = 0;
        for(int i = 0; i < children.size(); ++i) {
            SuperNode node = children.get(i);
            tempInternalDegree+=node.getInternalDegree();
            tempExternalDegree+=node.getExternalDegree();
            size+=node.getSize();
        }
        long totalDegree = tempInternalDegree+tempExternalDegree;
        double finalRatio = Math.min((totalDegree - tempInternalDegree )/ (double)totalDegree, externalRatio);
        long externalEdges[] = new long[children.size()];
        Arrays.fill(externalEdges, 0);
        
        /*externalDegree = (long)(totalDegree*finalRatio);
        for(int i = 0; i < children.size(); i++) {
            externalEdges[i] = children.get(i).getExternalDegree();
        }*/
        
        
        long externalDegreeBudget = Math.round(totalDegree*finalRatio);
        long totalToExternal = 0;
        
        for(int i = 0; i < children.size() && externalDegreeBudget > 0; i++) {
            SuperNode child = children.get(i);
            long toRemove = Math.min(Math.round(child.getExternalDegree()*0.25),externalDegreeBudget);
            externalEdges[i] = toRemove;
            externalDegreeBudget-=toRemove;
            totalToExternal+=toRemove;
        }
        externalDegree=totalToExternal;
    
        internalDegree = totalDegree-externalDegree;
        long intraDegree = tempInternalDegree;
        intraNodeEdgeProb = intraDegree / (double)(internalDegree);
    
        int numExternalNonZero = 0;
        for(int i = 0; i < children.size(); ++i) {
            if(externalEdges[i] > 0) {
                sampleNodeExternalIndices[numExternalNonZero] = i;
                numExternalNonZero++;
            }
        }
    
        sampleNodeExternalCumProbability = new double[numExternalNonZero];
        sampleNodeInternalProbability = new double[children.size()];
        sampleInterNodeProbability = new double[children.size()];
        interDegree = new long[children.size()];

        if(numExternalNonZero > 0) {
            sampleNodeExternalCumProbability[0] = 0.0;
            for (int i = 1; i < numExternalNonZero; ++i) {
                sampleNodeExternalCumProbability[i] = sampleNodeExternalCumProbability[i - 1] +
                    externalEdges[sampleNodeExternalIndices[i - 1]] /
                        (double) externalDegree;
            }
        }
    
        for (int i = 0; i < sampleInterNodeProbability.length; ++i) {
            SuperNode child = children.get(i);
            long childInternalDegree = child.getExternalDegree() - externalEdges[i];
            interDegree[i] = childInternalDegree;
            sampleInterNodeProbability[i] = (internalDegree - intraDegree) > 0 ? childInternalDegree /
                (double) (internalDegree - intraDegree) : 0.0;
        }
    
        for (int i = 0; i < sampleNodeInternalProbability.length; ++i) {
            SuperNode child = children.get(i);
            long childInternalDegree = child.getInternalDegree();
            sampleNodeInternalProbability[i] = childInternalDegree / (double) tempInternalDegree;
        }
        
        
        long childrenDegree = tempInternalDegree+tempExternalDegree;
        long currentDegree = internalDegree + externalDegree;
        
        if(childrenDegree != currentDegree) {
            throw new RuntimeException("Found inconcistency between children and current degree "+currentDegree+" " +
                                           ""+childrenDegree);
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
    public void sampleEdges(FileWriter writer, Random random, long numEdges, long offset) throws IOException {
        
        // Creating intra node edges
        long intraNodeEdges = Math.round(intraNodeEdgeProb * numEdges);
        for(int i = 0; i < children.size(); ++i) {
            SuperNode child = children.get(i);
            long numEdgesToCreate = (Math.round(intraNodeEdges * sampleNodeInternalProbability[i]));
            child.sampleEdges(writer, random, numEdgesToCreate, offset + offsets[i]);
        }
        
        long degreeBudget[] = Arrays.copyOf(interDegree, interDegree.length);
        
        boolean finished = false;
        
        while(!finished) {
            finished = true;
            for (int i = 0; i < degreeBudget.length; ++i) {
                for (int j = i + 1; j < degreeBudget.length && degreeBudget[i] > 0; ++j) {
                    if (degreeBudget[j] > 0) {
                        long node1 = children.get(i).sampleNode(random, offset + offsets[i]);
                        long node2 = children.get(j).sampleNode(random, offset + offsets[j]);
                        writer.write(node1 + "\t" + node2 + "\n");
                        degreeBudget[i]--;
                        degreeBudget[j]--;
                        finished = false;
                    }
                }
            }
        }
        
        long remainingEdges = 0;
        for(int i = 0; i < degreeBudget.length; ++i)  {
            remainingEdges+=degreeBudget[i];
        }
        remainingEdges /= 2;
        
        for(int i = 0; i < remainingEdges; ++i) {
            int first = random.nextInt(children.size());
            int second = random.nextInt(children.size());
            long node1 = children.get(first).sampleNode(random, offset + offsets[first]);
            long node2 = children.get(second).sampleNode(random, offset + offsets[second]);
            writer.write(node1 + "\t" + node2 + "\n");
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
        
        if(pos == -1) {
           pos = 0;
        }
        
        int index = sampleNodeExternalIndices[pos];
        return children.get(index).sampleNode(random, offset + offsets[index]);
    }
    
}
