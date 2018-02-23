package ldbc.snb.bteronhplus.structures;

import java.util.*;

public class BlockModel {

    final static String SEPARATOR = "\\|";
    final static float CLUSTER_THRESHOLD = 0.3f;

    private static class ModelEntry {

        public int id;
        public long size;
        public HashMap<Integer,Long> externalDegree = new HashMap<Integer,Long>();


        public ModelEntry() {
            id = 0;
            size = 0;
        }

        public ModelEntry(int id, String entry) {
            String fields[] = entry.split(SEPARATOR);
            this.id = id;
            this.size = Long.parseLong(fields[0]);
            for( int i = 1; i < fields.length; ++i) {
                String cell[] = fields[i].split(":");
                int idOther = Integer.parseInt(cell[0]);
                long degree = Long.parseLong(cell[1]);
                this.externalDegree.put(idOther,degree);
            }
        }

    }

    /**
     * The sizes ratio of the blocks
     */
    public double blockSizes[];

    /**
     * The number of external degree ratio per block
     */
    public double degree[][];

    /**
     * The total degree ratio (internal and external) per block
     */
    public double totalBlockDegree[];

    public BlockModel(double[] blockSizes, double[][] degrees) {
        assert( blockSizes.length == degrees.length);
        this.blockSizes = blockSizes;
        this.degree = degrees;
        this.totalBlockDegree = new double[blockSizes.length];
        Arrays.fill(this.totalBlockDegree, 0.0);
        for(int i = 0; i < this.blockSizes.length; ++i ) {
            for(int j = 0; j < this.blockSizes.length; ++j ) {
                this.totalBlockDegree[i] += this.degree[i][j];
            }
        }
    }

    public BlockModel(String model) {
        String[] lines = model.split("\n");

        // Parsing entries
        ArrayList<ModelEntry> entries = new ArrayList<ModelEntry>();
        for(int i = 0; i < lines.length; ++i) {
            entries.add(new ModelEntry(i,lines[i]));
        }

        // Sorting blocks by size
        Collections.sort(entries, new Comparator<ModelEntry>() {

            @Override
            public int compare(ModelEntry modelEntry1, ModelEntry modelEntry2) {
                if(modelEntry1.size == modelEntry2.size) return 0;
                return modelEntry1.size - modelEntry2.size > 0L? -1 : 1;
            }
        });

        int totalSize = 0;
        for (ModelEntry entry : entries) {
            totalSize += entry.size;
        }

        // Look for the first block that has to go to the cloud entry
        int currentSize = 0;
        int i = 0;
        for(; i < entries.size(); ++i ) {
            ModelEntry entry = entries.get(i);
            currentSize += entry.size;
            if( currentSize / (float)totalSize > CLUSTER_THRESHOLD || i > 10) {
                i+=1;
                break;
            }
        }

        HashMap<Integer,Integer> blockReMap = new HashMap<Integer,Integer>();
        for(int j = 0; j < entries.size(); ++j) {
            if(j < i ) {
                blockReMap.put(entries.get(j).id, j);
            } else { // If this entry is to be merged in the cloud entry
                blockReMap.put(entries.get(j).id, i);
            }
        }

        System.out.println("Number of communities in block after collapsing "+i);

        long blockSizes[] = new long[i+1];
        Arrays.fill(blockSizes,0L);

        long edges[][] = new long[i+1][i+1];
        for( long[] array : edges) {
            Arrays.fill(array,0L);
        }

        long totalBlockEdges[] = new long[i+1];
        Arrays.fill(totalBlockEdges,0L);

        long totalDegree = 0;
        for(ModelEntry entry : entries) {
            int newId = blockReMap.get(entry.id);
            blockSizes[newId] += entry.size;
            for(HashMap.Entry<Integer,Long> cell : entry.externalDegree.entrySet()) {
                int otherId = blockReMap.get(cell.getKey());
                edges[newId][otherId] += cell.getValue();
                totalBlockEdges[newId] += cell.getValue();
                totalDegree+=cell.getValue();
            }
        }

        this.blockSizes = new double[i+1];
        for(int j = 0; j < this.blockSizes.length; ++j) {
            this.blockSizes[j] = blockSizes[j] / (double)totalSize;
        }

        this.degree = new double[i+1][i+1];
        for(int j = 0; j < this.degree.length; ++j) {
            for(int k = 0; k < this.degree[j].length; ++k) {
                this.degree[j][k] = edges[j][k] / (double)totalDegree;
            }
        }

        this.totalBlockDegree = new double[i+1];
        for(int j = 0; j < this.totalBlockDegree.length; ++j) {
            this.totalBlockDegree[j] = totalBlockEdges[j] / (double)totalDegree;
        }

    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < this.blockSizes.length; ++i) {
            builder.append(this.blockSizes[i]);
            for( int j = 0; j < this.degree.length; ++j) {
                for( int k = 0; k < this.degree[j].length; ++k) {
                    if(degree[j][k] != 0) {
                        builder.append(k+":"+degree[j][k]+"|");
                    }
                }
            }
            builder.replace(builder.length()-1,builder.length(), "\n");
        }
        return builder.toString();
    }
}
