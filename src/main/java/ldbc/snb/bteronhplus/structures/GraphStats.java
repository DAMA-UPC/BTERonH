package ldbc.snb.bteronhplus.structures;

import ldbc.snb.bteronhplus.tools.FileTools;
import org.apache.hadoop.conf.Configuration;
import org.python.antlr.ast.arguments;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class GraphStats {

    private HashMap<Integer, EmpiricalDistribution> degreeDistribution = null;
    private HashMap<Integer, EmpiricalDistribution> ccPerDegree = null;
    private EmpiricalDistribution communitySizeDistribution = null;
    private HashMap<Integer,Double> communityDensityMap = null;
    private Set<Integer> communitySizes = null;

    public GraphStats(String degreeSequenceFile,
                      String ccPerDegreeSequenceFile,
                      String communitySizeSequenceFile,
                      String communityDensityMapFile
                      ) {


        Configuration conf  = new Configuration();
        degreeDistribution  = new HashMap<Integer, EmpiricalDistribution>();
        ccPerDegree         = new HashMap<Integer, EmpiricalDistribution>();
        communitySizeDistribution = new EmpiricalDistribution(communitySizeSequenceFile, conf, 0);
        communityDensityMap = new HashMap<Integer,Double>();
        communitySizes      = new HashSet<Integer>();

        // Loading degree distribution per community size
        try {
            BufferedReader reader = FileTools.getFile(degreeSequenceFile, conf);
            String line;
            line = reader.readLine();
            while (line != null) {
                String[] numbers = line.split(" ");
                int degree = (int)Double.parseDouble(numbers[0]);
                ArrayList<Double> observedSequence = new ArrayList<Double>();
                for (int i = 1; i < numbers.length; ++i) {
                    observedSequence.add(Double.parseDouble(numbers[i]));
                }

                degreeDistribution.put(degree, new EmpiricalDistribution(observedSequence));
                line = reader.readLine();
            }
        } catch (IOException e ) {
            e.printStackTrace();
            System.exit(1);
        }

        // Loading clustering coefficient per degree distributions
        try {
            BufferedReader reader = FileTools.getFile(ccPerDegreeSequenceFile, conf);
            String line;
            line = reader.readLine();
            while (line != null) {
                String[] numbers = line.split(" ");
                int degree = (int)Double.parseDouble(numbers[0]);
                ArrayList<Double> observedSequence = new ArrayList<Double>();
                for (int i = 1; i < numbers.length; ++i) {
                    observedSequence.add(Double.parseDouble(numbers[i]));
                }
        
                if(observedSequence.size() == 1) {
                    observedSequence.add(observedSequence.get(0));
                }
                ccPerDegree.put(degree, new EmpiricalDistribution(observedSequence));
                line = reader.readLine();
            }
        } catch (IOException e ) {
            e.printStackTrace();
            System.exit(1);
        }


        // Community density map file
        try {
            BufferedReader reader = FileTools.getFile(communityDensityMapFile, conf);
            String line;
            line = reader.readLine();
            while (line != null) {
                String[] numbers = line.split(" ");
                int degree = (int)Double.parseDouble(numbers[0]);
                double density = (double)Double.parseDouble(numbers[1]);
                communityDensityMap.put(degree,density);
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Loading community size distribution
        try {
            BufferedReader reader = FileTools.getFile(communitySizeSequenceFile, conf);
            String line;
            line = reader.readLine();
            while (line != null) {
                communitySizes.add((int)(double)Double.parseDouble(line));
                line = reader.readLine();
            }
        } catch (IOException e ) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    public EmpiricalDistribution getDegreeDistribution(Integer communitySize) {
        return degreeDistribution.get(communitySize);
    }

    public EmpiricalDistribution getCommunitySizeDistribution() {
        return communitySizeDistribution;
    }

    public EmpiricalDistribution getCCPerDegreeDistribution(int degree) {
        return ccPerDegree.get(degree);
    }

    public double getCommunityDensity(int size) {
        Double density = communityDensityMap.get(size);
        if(density == null) {
            for (int i = size-1; i >= 0; --i) {
                density = communityDensityMap.get(i);
                if(density != null) {
                    communityDensityMap.put(size,density);
                    break;
                }
            }
        }
        return density;
    }

    public Set<Integer> getCommunitySizes() {
        return communitySizes;
    }
}
