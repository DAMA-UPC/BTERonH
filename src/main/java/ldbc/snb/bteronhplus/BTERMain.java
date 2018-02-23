package ldbc.snb.bteronhplus;

import com.beust.jcommander.JCommander;
import ldbc.snb.bteronhplus.algorithms.BTER;
import ldbc.snb.bteronhplus.algorithms.Partitioning;
import ldbc.snb.bteronhplus.hadoop.HadoopBTERGenerator;
import ldbc.snb.bteronhplus.hadoop.HadoopBTERPlusGenerator;
import ldbc.snb.bteronhplus.hadoop.HadoopEdgePartitioner;
import ldbc.snb.bteronhplus.structures.Arguments;
import ldbc.snb.bteronhplus.structures.BTERPlusStats;
import ldbc.snb.bteronhplus.structures.BTERStats;
import ldbc.snb.bteronhplus.structures.BlockModel;
import ldbc.snb.bteronhplus.tools.FileTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class BTERMain {

    private static void writeMapTriggerFile(String filename, int numMaps, Configuration conf) {
        try {
            FileSystem dfs = FileSystem.get(conf);
            OutputStream output = dfs.create(new Path(filename));
            for (int i = 0; i < numMaps; i++)
                output.write((new String(i + "\n").getBytes()));
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String [] args) throws Exception {

        System.out.println("Generating edges");
        Configuration conf = new Configuration();
        conf.setInt("ldbc.snb.bteronh.generator.numThreads",1);
        conf.setLong("ldbc.snb.bteronh.generator.numNodes",10000);
        conf.setInt("ldbc.snb.bteronh.generator.seed",12323540);
        conf.set("ldbc.snb.bteronh.serializer.workspace","./");
        conf.set("ldbc.snb.bteronh.generator.degreeSequence","");
        conf.set("ldbc.snb.bteronh.generator.ccPerDegree","");
        conf.set("ldbc.snb.bteronh.generator.communities","");


        Arguments arguments = new Arguments();
        new JCommander(arguments,args);
        for(Arguments.Property p : arguments.getProperties()) {
            conf.set(p.getProperty(),p.getValue());
        }

        for(String propertyFile : arguments.getPropertyFiles()) {
            Properties properties = new Properties();
            try {
                properties.load(new FileReader(propertyFile));
            } catch(IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
            for( String s : properties.stringPropertyNames()) {
                conf.set(s,properties.getProperty(s));
            }
        }

        if(conf.get("ldbc.snb.bteronh.generator.degreeSequence").compareTo("") == 0) {
            System.out.println("Degree sequence file missing. Need to specify one via ldbc.snb.bteronh.generator" +
                    ".degreeSequence option");
            System.exit(1);
        }

        if(conf.get("ldbc.snb.bteronh.generator.ccPerDegree").compareTo("") == 0) {
            System.out.println("CC per degree file missing. Need to specify one via ldbc.snb.bteronh" +
                    ".generator.ccPerDegree option");
            System.exit(1);
        }

        if(conf.get("ldbc.snb.bteronh.generator.communities").compareTo("") == 0) {
            System.out.println("Community structure file missing. Need to specify one via ldbc.snb.bteronh" +
                    ".generator.communities option");
            System.exit(1);
        }


        /** Printing Options **/
        System.out.println("ldbc.snb.bteronh.generator.numThreads "+conf.get("ldbc.snb.bteronh.generator.numThreads"));
        System.out.println("ldbc.snb.bteronh.generator.numNodes "+conf.get("ldbc.snb.bteronh.generator.numNodes"));
        System.out.println("ldbc.snb.bteronh.generator.seed "+conf.get("ldbc.snb.bteronh.generator.seed"));
        System.out.println("ldbc.snb.bteronh.serializer.workspace "+conf.get("ldbc.snb.bteronh.serializer.workspace"));
        System.out.println("ldbc.snb.bteronh.generator.degreeSequence "+conf.get("ldbc.snb.bteronh.generator" +
                ".degreeSequence"));
        System.out.println("ldbc.snb.bteronh.generator.ccPerDegree "+conf.get("ldbc.snb.bteronh.generator" +
                ".ccPerDegree"));

        System.out.println("ldbc.snb.bteronh.generator.communities "+conf.get("ldbc.snb.bteronh.generator" +
                ".communities"));


        System.out.println("Starting execution");
        long start = System.currentTimeMillis();
        int seed = conf.getInt("ldbc.snb.bteronh.generator.seed",0);
        int numThreads = conf.getInt("ldbc.snb.bteronh.generator.numThreads",1);
        long numNodes = conf.getLong("ldbc.snb.bteronh.generator.numNodes",10000);
        String degreeSequenceFile = conf.get("ldbc.snb.bteronh.generator.degreeSequence");
        String ccPerDegreeFile = conf.get("ldbc.snb.bteronh.generator.ccPerDegree");
        String workSpace = FileTools.isHDFSPath(conf.get("ldbc.snb.bteronh.serializer.workspace"));
        if(workSpace == null) {
            throw new IOException("Ill-formed workspace URI. Workspace must start with hdfs://");
        }
        conf.set("ldbc.snb.bteronh.serializer.dataDir",workSpace+"/data");
        conf.set("ldbc.snb.bteronh.serializer.hadoopDir",workSpace+"/hadoop");
        String hadoopDir = new String( conf.get("ldbc.snb.bteronh.serializer.hadoopDir"));
        String dataDir = new String( conf.get("ldbc.snb.bteronh.serializer.dataDir"));
        String mapTriggerFile = hadoopDir+"/mrInputFile";

        String outputFileName = conf.get("ldbc.snb.bteronh.serializer.outputFileName");
        if(outputFileName != null){
            outputFileName = FileTools.isHDFSPath(outputFileName);
            if(outputFileName == null)  {
                throw new IOException("Ill-formed outputFileName URI"+outputFileName+". OutputFileName must start " +
                        "with hdfs://");
            }
        } else {
            throw new IOException("You need to specify. ldbc.snb.bteronh.serializer.outputFileName");
        }

        System.out.println(dataDir);
        System.out.println(hadoopDir);
        System.out.println(outputFileName);

        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(hadoopDir), true);
        dfs.delete(new Path(dataDir), true);
        dfs.delete(new Path(mapTriggerFile), true);
        writeMapTriggerFile(mapTriggerFile, Integer.parseInt(conf.get("ldbc.snb.bteronh.generator.numThreads")), conf);


        System.out.println("Starting generation of degrees and communities");

        BufferedReader reader = null;
        ArrayList<Integer> observedDegreeSequence = new ArrayList<Integer>();
        try {
            reader = FileTools.getFile(degreeSequenceFile, conf);
            String line;
            line = reader.readLine();
            while(line!=null) {
                observedDegreeSequence.add(Integer.parseInt(line));
                line = reader.readLine();
            }
        } catch (IOException exception) {
            exception.printStackTrace();
            System.exit(1);
        }

        HashMap<Integer, Double> ccPerDegree = new HashMap<Integer,Double>();
        try {
            reader = FileTools.getFile(ccPerDegreeFile, conf);
            String line = reader.readLine();
            while (line != null) {
                String[] splitLine = line.split(" ");
                ccPerDegree.put(Integer.parseInt(splitLine[0]), Double.parseDouble(splitLine[1]));
                line = reader.readLine();
            }
        } catch (IOException exception) {
            exception.printStackTrace();
            System.exit(1);
        }

        RandomVariateGen randomVariateGen = BTER.getDegreeSequenceSampler(observedDegreeSequence, numNodes, seed);

        System.out.println("Generating Degree Sequence");
        HashMap<Integer,Long> degrees = new HashMap<Integer,Long>();
        for(long i = 0; i < numNodes; ++i) {
            int degree = (int)randomVariateGen.nextDouble();
            degrees.compute(degree,(k,v)-> v == null ? 1 : v + 1 );
            if(i % 1000000 == 0) {
                System.out.println("Generated "+i+" degrees");
            }
        }

        System.out.println("Configuring communities partition");

        String communitiesFilePath = FileTools.isLocalPath(conf.get("ldbc.snb.bteronh.generator.communities"));
        if( communitiesFilePath == null) {
            System.out.println("Invalid path for communities file. It must be a local file system path");
            System.exit(1);
        }


        byte[] encoded = Files.readAllBytes(Paths.get(communitiesFilePath));
        String communityStructure = new String(encoded);

        System.out.println("Preparing block model");
        BlockModel blockModel = new BlockModel(communityStructure);

        System.out.println("Distributing degrees");
        List<HashMap<Integer,Long>> partition = Partitioning.partition(blockModel, randomVariateGen, numNodes);

        double [] partitionSizes = new double[partition.size()];
        double [] partitionDegrees = new double[partition.size()];

        Partitioning.partitionToBlockSizes(partition, partitionSizes, partitionDegrees);
        for(int i = 0; i < partition.size(); ++i) {
            System.out.println(blockModel.blockSizes[i]+" "+partitionSizes[i]+" "+blockModel.totalBlockDegree[i]+" " +
                    ""+partitionDegrees[i]);
        }

        System.out.println("Found "+partition.size()+" communities");

        Long offsets[] = new Long[partition.size()];
        long externalDegreePerCommunity[]  = new long[partition.size()];
        Arrays.fill(externalDegreePerCommunity,0);
        int communityIndex = 0;
        long totalGeneratedNodes = 0;
        long totalDegree = 0;

        for(HashMap<Integer,Long> community : partition ) {
            offsets[communityIndex] = new Long(totalGeneratedNodes);

            long nodesToGenerate = 0;
            for(Map.Entry<Integer,Long> counts : community.entrySet()) {
                nodesToGenerate+=counts.getValue();
                totalDegree+= counts.getKey()*counts.getValue();
            }
            totalGeneratedNodes+=nodesToGenerate;
            communityIndex++;
        }

        for(int i = 0; i < partition.size(); ++i) {
            for(int j = 0; j < partition.size(); ++j) {
                if(i != j) {
                    externalDegreePerCommunity[i] += (long) (blockModel.degree[i][j] * totalDegree);
                }
            }
        }

        System.out.println("Total expected degree "+totalDegree);

        String ccMapFile = hadoopDir + "/ccmap";
        conf.set("ldbc.snb.bteronh.generator.ccmap", ccMapFile);
        FileTools.serializeHashMap(ccPerDegree, ccMapFile, conf);

        communityIndex = 0;

        BTERPlusStats bterPlusStats[] = new BTERPlusStats[partition.size()];

        for(HashMap<Integer,Long> community : partition ) {
            System.out.println("Starting BTER generation of community "+communityIndex);

            BTERStats stats = new BTERStats();
            stats.initialize(community,ccPerDegree, externalDegreePerCommunity[communityIndex]);

            System.out.println("External Degree Community " +
                    ""+communityIndex+":"+externalDegreePerCommunity[communityIndex]);

            bterPlusStats[communityIndex] = new BTERPlusStats();
            bterPlusStats[communityIndex].initialize(community,stats.getExternalDegree(),communityIndex,blockModel);

            String bterStatsFile = hadoopDir + "/bterstats"+communityIndex;
            conf.set("ldbc.snb.bteronh.generator.bterstats", bterStatsFile);
            FileTools.serializeObject(stats, bterStatsFile, conf);

            conf.setInt("communityId",communityIndex);
            conf.setLong("offset",offsets[communityIndex]);

            conf.setInt("mapreduce.input.lineinputformat.linespermap", 1);
            Job job = Job.getInstance(conf, "Generating Graph");
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(LongWritable.class);
            job.setJarByClass(HadoopBTERGenerator.HadoopBTERGeneratorMapper.class);
            job.setMapperClass(HadoopBTERGenerator.HadoopBTERGeneratorMapper.class);
            job.setReducerClass(HadoopBTERGenerator.HadoopBTERGeneratorReducer.class);
            job.setNumReduceTasks(numThreads);
            job.setInputFormatClass(NLineInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setPartitionerClass(HadoopEdgePartitioner.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.setInputPaths(job, new Path(mapTriggerFile));
            FileOutputFormat.setOutputPath(job, new Path(outputFileName+"."+communityIndex));
            if (!job.waitForCompletion(true)) {
                throw new Exception(job.toString());
            }
            communityIndex++;
        }


        String bterPlusStatsFile = hadoopDir + "/bterplusstats";
        conf.set("ldbc.snb.bteronh.generator.bterplusstats", bterPlusStatsFile);
        FileTools.serializeObjectArray(bterPlusStats, bterPlusStatsFile, conf);

        String offsetsFile = hadoopDir + "/offsets";
        conf.set("ldbc.snb.bteronh.generator.offsets", offsetsFile);
        FileTools.serializeObjectArray(offsets, offsetsFile, conf);

        System.out.println("Starting BTER inter-community edge generation "+communityIndex);

        conf.setInt("mapreduce.input.lineinputformat.linespermap", 1);
        Job job = Job.getInstance(conf, "Generating Graph");
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setJarByClass(HadoopBTERPlusGenerator.HadoopBTERPlusGeneratorMapper.class);
        job.setMapperClass(HadoopBTERPlusGenerator.HadoopBTERPlusGeneratorMapper.class);
        job.setReducerClass(HadoopBTERPlusGenerator.HadoopBTERPlusGeneratorReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(HadoopEdgePartitioner.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(mapTriggerFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFileName+".inter"));
        if (!job.waitForCompletion(true)) {
            throw new Exception(job.toString());
        }

        long end = System.currentTimeMillis();
        System.out.println("Execution time: "+(end-start)/1000.0+" second");

    }
}
