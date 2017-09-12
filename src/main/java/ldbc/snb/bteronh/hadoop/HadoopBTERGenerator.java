package ldbc.snb.bteronh.hadoop;

import javafx.util.Pair;
import ldbc.snb.bteronh.algorithms.Algorithms;
import ldbc.snb.bteronh.structures.BTERStats;
import ldbc.snb.bteronh.structures.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created by aprat on 16/08/16.
 */
public class HadoopBTERGenerator {

    protected static String isHDFSPath(String fileName) {
        if (fileName.startsWith("hdfs://")) {
            return fileName.substring(7);
        }
        return null;
    }

    protected static String isLocalPath(String fileName) {
        if (fileName.startsWith("file://")) {
            return fileName.substring(7);
        }
        return null;
    }

    public static class HadoopBTERGeneratorMapper  extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

        private BufferedReader getFile(String fileName, Configuration conf) throws IOException {
            String realFileName;
            if ((realFileName = isHDFSPath(fileName)) != null) {
                FileSystem fs = FileSystem.get(conf);
                return new BufferedReader( new InputStreamReader(fs.open( new Path(realFileName))));
            } else if((realFileName = isLocalPath(fileName)) != null) {
                return new BufferedReader(new FileReader(realFileName));
            } else {
                throw new IOException("Invalid file URI "+fileName+". It must start with hdfs:// or file://");
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int threadId = Integer.parseInt(value.toString());
            int seed = conf.getInt("ldbc.snb.bteronh.generator.seed",0);
            int numThreads = conf.getInt("ldbc.snb.bteronh.generator.numThreads",1);
            long numNodes = conf.getLong("ldbc.snb.bteronh.generator.numNodes",10000);
            String degreeSequenceFile = conf.get("ldbc.snb.bteronh.generator.degreeSequence");
            String ccPerDegreeFile = conf.get("ldbc.snb.bteronh.generator.ccPerDegree");

            BufferedReader reader = getFile(degreeSequenceFile,conf);
            ArrayList<Integer> observedDegreeSequence = new ArrayList<Integer>();
            String line;
            line = reader.readLine();
            while(line!=null) {
                observedDegreeSequence.add(Integer.parseInt(line));
                line = reader.readLine();
            }

            reader = getFile(ccPerDegreeFile,conf);
            ArrayList<Pair<Long,Double>> ccPerDegree = new ArrayList<Pair<Long,Double>>();
            line = reader.readLine();
            while(line!=null) {
                String [] splitLine = line.split(" ");
                ccPerDegree.add( new Pair<Long,Double>(Long.parseLong(splitLine[0]), Double.parseDouble(splitLine[1])));
                line = reader.readLine();
            }

            System.out.println("Initializing BTER stats");
            BTERStats stats = new BTERStats();
            stats.initialize(numNodes, observedDegreeSequence, ccPerDegree, seed, (i) -> context.setStatus("Generated "+i+" degrees at mapper "+threadId));

            long totalWeight = stats.getWeightPhase1()+stats.getWeightPhase2();
            long blockSize =  totalWeight / numThreads;
            if(threadId == 0) {
                long residual = totalWeight % numThreads;
                blockSize += residual;
            }

            System.out.println("Mapper "+threadId+": Generating "+blockSize+" edges out of "+totalWeight);
            Random random = new Random();
            random.setSeed(seed+threadId);
            for(long i = 0; i < blockSize; ++i) {
                Edge edge = Algorithms.BTERSample(stats,random);
                context.write(new LongWritable(edge.getTail()), new LongWritable(edge.getHead()));
                if( i % 100000 == 0 ) {
                    context.setStatus("Generated "+i+" edges out of "+blockSize+" in mapper "+threadId);
                }
            }
        }
    }


    public static class HadoopBTERGeneratorReducer  extends Reducer<LongWritable,LongWritable, LongWritable, LongWritable> {

        @Override
        public void reduce(LongWritable tail, Iterable<LongWritable> valueSet,
                           Context context) throws IOException, InterruptedException {
            Set<Long> neighbors = new HashSet<Long>();
            for ( LongWritable head : valueSet ) {
                neighbors.add(head.get());
            }

            for( Long head : neighbors) {
                String str = new String(tail + " " + head + "\n");
                context.write(tail,new LongWritable(head));
            }
        }
    }

    private Configuration conf = null;

    public HadoopBTERGenerator(Configuration conf ) {
        this.conf  = new Configuration(conf);
    }

    private static void writeToOutputFile(String filename, int numMaps, Configuration conf) {
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

    public void run() throws Exception {

        String workSpace = isHDFSPath(conf.get("ldbc.snb.bteronh.serializer.workspace"));
        if(workSpace == null) {
            throw new IOException("Ill-formed workspace URI. Workspace must start with hdfs://");
        }
        conf.set("ldbc.snb.bteronh.serializer.dataDir",workSpace+"/data");
        conf.set("ldbc.snb.bteronh.serializer.hadoopDir",workSpace+"/hadoop");
        String hadoopDir = new String( conf.get("ldbc.snb.bteronh.serializer.hadoopDir"));
        String dataDir = new String( conf.get("ldbc.snb.bteronh.serializer.dataDir"));
        String tempFile = hadoopDir+"/mrInputFile";

        String outputFileName = conf.get("ldbc.snb.bteronh.serializer.outputFileName");
        if(outputFileName != null){
            outputFileName = isHDFSPath(outputFileName);
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
        dfs.delete(new Path(tempFile), true);
        writeToOutputFile(tempFile, Integer.parseInt(conf.get("ldbc.snb.bteronh.generator.numThreads")), conf);

        int numThreads = Integer.parseInt(conf.get("ldbc.snb.bteronh.generator.numThreads"));
        conf.setInt("mapreduce.input.lineinputformat.linespermap", 1);
        Job job = Job.getInstance(conf, "Generating Graph");
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setJarByClass(HadoopBTERGeneratorMapper.class);
        job.setMapperClass(HadoopBTERGeneratorMapper.class);
        job.setReducerClass(HadoopBTERGeneratorReducer.class);
        job.setNumReduceTasks(numThreads);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(HadoopEdgePartitioner.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(tempFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFileName));
        if(!job.waitForCompletion(true)) {
            throw new Exception(job.toString());
        }
    }
}
