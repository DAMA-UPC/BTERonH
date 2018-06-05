package ldbc.snb.bteronhplus.structures;

import ldbc.snb.bteronhplus.tools.FileTools;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class RealCommunityStreamer implements SuperNodeStreamer {
    
    private HashMap<Integer, List<Community>> models;
    private GraphStats stats;
    private Random random;
    
    public RealCommunityStreamer(GraphStats stats, String communitiesFile, String degreeFile)  {
        random = new Random();
        this.stats = stats;
        HashMap<Integer, Integer> degrees =  new HashMap<Integer, Integer>();
        Configuration conf  = new Configuration();
        this.models = new HashMap<Integer, List<Community>>();
        try {
    
            BufferedReader reader = FileTools.getFile(degreeFile, conf);
            String line;
            line = reader.readLine();
            while (line != null) {
                String[] node = line.split(" ");
                degrees.put(Integer.parseInt(node[0]), Integer.parseInt(node[1]));
                line = reader.readLine();
            }
            reader.close();
            
            reader = FileTools.getFile(communitiesFile, conf);
            line = reader.readLine();
            while (line != null) {
                HashMap<Integer, Integer> idMap = new HashMap<Integer, Integer>();
                HashMap<Integer, Integer> localDegrees = new HashMap<Integer,Integer>();
                int nextId = 0;
                ArrayList<Edge> edges = new ArrayList<Edge>();
                String[] community = line.split(" ");
                for (int i = 0; i < community.length; ++i) {
                    String[] endpoints = community[i].split(":");
                    int tail = Integer.parseInt(endpoints[0]);
                    int head = Integer.parseInt(endpoints[1]);
                    if(idMap.get(tail) == null) {
                        idMap.put(tail, nextId);
                        localDegrees.put(nextId, degrees.get(tail));
                        nextId++;
                    }
                    tail = idMap.get(tail);
    
                    if(idMap.get(head) == null) {
                        idMap.put(head, nextId);
                        localDegrees.put(nextId, degrees.get(head));
                        nextId++;
                    }
                    head = idMap.get(head);
                    
                    edges.add(new Edge(tail, head));
                }
    
                Map<Integer, Integer > degree = new HashMap<Integer, Integer>();
                for(Edge edge : edges) {
                    degree.merge((int)edge.getTail(), 1 , Integer::sum);
                    degree.merge((int)edge.getHead(), 1 , Integer::sum);
                }
    
                int size = degree.keySet().size();
                int excessDegree[] = new int[size];
                for(Map.Entry<Integer,Integer> entry : degree.entrySet()) {
                    Integer nodeDegree = localDegrees.get(entry.getKey());
                    excessDegree[entry.getKey()] = (int) Math.max(0.0, nodeDegree - entry
                        .getValue());
                }
                
                List<Community> communities = models.get(size);
                if(communities == null) {
                    communities = new ArrayList<Community>();
                    models.put(size, communities);
                }
                communities.add(new Community(0, size, edges, excessDegree));
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e ) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    @Override
    public SuperNode next() {
        EmpiricalDistribution sizeDistribution = stats.getCommunitySizeDistribution();
        int nextSize = (int)sizeDistribution.getNext();
        List<Community> communities = models.get(nextSize);
        Community community = communities.get(random.nextInt(communities.size()));
        return community;
    }
}
