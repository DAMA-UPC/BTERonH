package ldbc.snb.bteronhplus.structures;

import java.util.*;

public class BlockModel {

    final static String SEPARATOR = "\\|";
    final static float CLUSTER_THRESHOLD = 0.8f;
    final static int   MAX_BLOCKS = 2000;

    public static class ModelEntry {

        public long id;
        public double size;
        public HashMap<Long,Double> degree = new HashMap<Long,Double>();
        public double totalDegree = 0.0;
        public double externalDegree = 0.0;


        public ModelEntry() {
            id = 0;
            size = 0;
        }

        public ModelEntry(String entry) {
            String fields[] = entry.split(SEPARATOR);
            this.id = Long.parseLong(fields[0]);
            this.size = Double.parseDouble(fields[1]);
            for( int i = 2; i < fields.length; ++i) {
                String cell[] = fields[i].split(":");
                long idOther = Long.parseLong(cell[0]);
                double degree = Double.parseDouble(cell[1]);
                this.degree.put(idOther,degree);
                totalDegree+=degree;
                if(idOther != this.id) {
                    externalDegree += degree;
                }
            }
        }

    }

    Map<Long, ModelEntry> entries = new HashMap<Long, ModelEntry>();
    Map<Long, List<Long>> children = new HashMap<Long, List<Long>>();

    public BlockModel(String blockmodel, String childrenmodel) {
        String[] lines = blockmodel.split("\n");

        // Parsing entries
        for(int i = 0; i < lines.length; ++i) {
            ModelEntry entry = new ModelEntry(lines[i]);
            entries.put(entry.id, entry);
        }

        lines = childrenmodel.split("\n");
        for(int i = 0; i < lines.length; ++i) {
            String line[] = lines[i].split(SEPARATOR);
            Long id = Long.parseLong(line[0]);
            List<Long> blockChildren = children.get(id);
            if(blockChildren == null) {
                blockChildren = new ArrayList<Long>();
                children.put(id,blockChildren);
            }

            for(int j = 1; j < line.length; ++j) {
                blockChildren.add(Long.parseLong(line[j]));
            }
        }
    }

    public static BlockModel identity() {
        return new BlockModel("0|1.0|0:1.0", "0|0");
    }

    public Map<Long, ModelEntry> getEntries() {
        return entries;
    }

    public void setChildren(Map<Long,List<Long>> children) {
        this.children = children;
    }

    public int getNumBlocks() {
        return entries.size();
    }

    public Map<Long, List<Long>> getChildren() {
        return children;
    }
}
