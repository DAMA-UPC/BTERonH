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
    private HashMap<Integer, EmpiricalDistribution> ccDistributions = null;
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
        ccDistributions     = new HashMap<Integer, EmpiricalDistribution>();
        communitySizeDistribution = new EmpiricalDistribution(communitySizeSequenceFile, conf);
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

                /*if(numbers.length <= 2) { // This is needed because EmpiricalDist requires at least 2 observations
                    observedSequence.add(Double.parseDouble(numbers[1]));
                }*/

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

                if(numbers.length <= 2) { // This is needed because EmpiricalDist requires at least 2 observations
                    observedSequence.add(Double.parseDouble(numbers[1]));
                }

                ccDistributions.put(degree, new EmpiricalDistribution(observedSequence));
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
        EmpiricalDistribution distribution =  ccDistributions.get(degree);
        if(distribution == null) {
            for (int i = degree-1; i >= 0; --i) {
                distribution = ccDistributions.get(i);
                if(distribution != null) {
                    ccDistributions.put(degree,distribution);
                    break;
                }
            }
        }

        return distribution;
    }

    public Double getCommunityDensity(int size) {
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
