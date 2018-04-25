package ldbc.snb.bteronhplus.structures;

import java.util.Iterator;
import java.util.List;

public class SuperNodeClusterStreamer implements SuperNodeStreamer {

    private List<SuperNode> clusters;
    private Iterator<SuperNode> iter;

    public SuperNodeClusterStreamer (List<SuperNode> clusters) {
        this.clusters = clusters;
        this.iter = clusters.iterator();
    }

    @Override
    public SuperNode next() {
        return iter.next();
    }
}
