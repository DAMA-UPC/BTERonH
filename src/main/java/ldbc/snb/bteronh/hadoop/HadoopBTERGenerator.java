package ldbc.snb.bteronh.hadoop;

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

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created by aprat on 16/08/16.
 */
public class HadoopBTERGenerator {
    public static class HadoopBTERGeneratorMapper  extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int threadId = Integer.parseInt(value.toString());
            int seed = conf.getInt("ldbc.snb.bteronh.generator.seed",0);
            int numThreads = conf.getInt("ldbc.snb.bteronh.generator.numThreads",1);
            int numNodes = conf.getInt("ldbc.snb.bteronh.generator.numNodes",10000);
            String hadoopDir = new String( conf.get("ldbc.snb.bteronh.serializer.hadoopDir"));
            String degreeSequenceFile = hadoopDir+"/degreeSequence";
            String ccPerDegreeFile = hadoopDir+"/ccs";

            int [] degreeSequence = Algorithms.GenerateDegreeSequence(degreeSequenceFile, numNodes, seed);

            int maxDegree = Integer.MIN_VALUE;
            for(int i = 0; i < degreeSequence.length; ++i) {
                maxDegree = degreeSequence[i] > maxDegree ? degreeSequence[i] : maxDegree;
            }

            double [] cc = Algorithms.GenerateCCperDegree(ccPerDegreeFile,maxDegree);

            BTERStats stats = new BTERStats();
            stats.initialize(degreeSequence,cc);

            int totalWeight = stats.getWeightPhase1()+stats.getWeightPhase2();
            int blockSize =  totalWeight / numThreads;
            if(threadId == 0) {
                int residual = totalWeight % numThreads;
                blockSize += residual;
            }

            System.out.println("Mapper "+threadId+": Generating "+blockSize+" edges out of "+totalWeight);
            Random random = new Random();
            random.setSeed(seed+threadId);
            for(int i = 0; i < blockSize; ++i) {
                Edge edge = Algorithms.BTERSample(stats,random);
                context.write(new LongWritable(edge.getTail()), new LongWritable(edge.getHead()));
            }
        }
    }


    public static class HadoopBTERGeneratorReducer  extends Reducer<LongWritable,LongWritable, LongWritable, LongWritable> {

        private OutputStream outputStream  = null;

        @Override
        public void setup(Context context) {
            try {
                Configuration conf = context.getConfiguration();
                String dataDir = conf.get("ldbc.snb.bteronh.serializer.dataDir");
                int reducerId = context.getTaskAttemptID().getTaskID().getId();

                FileSystem fs = FileSystem.get(conf);
                outputStream = fs.create(new Path(dataDir + "/edges_"+ reducerId), true, 131072);
            } catch (IOException e) {
                System.err.println(e.getMessage());
                System.exit(-1);
            }

        }

        @Override
        public void reduce(LongWritable tail, Iterable<LongWritable> valueSet,
                           Context context) throws IOException, InterruptedException {
            Set<Long> neighbors = new HashSet<Long>();
            for ( LongWritable head : valueSet ) {
                neighbors.add(head.get());
            }

            for( Long head : neighbors) {
                String str = new String(tail + " " + head + "\n");
                outputStream.write(str.getBytes());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputStream.close();
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

        String hadoopDir = new String( conf.get("ldbc.snb.bteronh.serializer.hadoopDir"));
        String tempFile = hadoopDir+"/mrInputFile";

        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(tempFile), true);
        writeToOutputFile(tempFile, Integer.parseInt(conf.get("ldbc.snb.bteronh.generator.numThreads")), conf);

        dfs.copyFromLocalFile(new Path(conf.get("ldbc.snb.bteronh.generator.degreeSequence")),
                new Path(hadoopDir+"/degreeSequence"));

        dfs.copyFromLocalFile(new Path(conf.get("ldbc.snb.bteronh.generator.ccPerDegree")),
                new Path(hadoopDir+"/ccs"));

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
        FileInputFormat.setInputPaths(job, new Path(tempFile));
        FileOutputFormat.setOutputPath(job, new Path(hadoopDir+"/blackhole"));
        if(!job.waitForCompletion(true)) {
            throw new Exception();
        }
        dfs.delete(new Path(hadoopDir+"/blackhole"),true);
    }
}
