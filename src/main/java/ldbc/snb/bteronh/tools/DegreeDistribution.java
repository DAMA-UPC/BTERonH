package ldbc.snb.bteronh.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.*;
import java.util.*;

/**
 * Created by aprat on 16/08/16.
 */
public class DegreeDistribution {

    public static class Arguments {

        @Parameter(names = "-inputfiles", variableArity = true, required = true)
        public List<String> inputfiles = new ArrayList<>();

        @Parameter(names = "-outputfolder", required = true)
        public String outputfolder = new String();

        @Parameter(names = "-numthreads")
        public int numthreads = 1;

    }

    public static class EdgeMapper  extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String splitedLine [] = line.split(" ");
            long tail = Long.parseLong(splitedLine[0]);
            long head = Long.parseLong(splitedLine[1]);
            context.write(new LongWritable(tail), new LongWritable(head));
            context.write(new LongWritable(head), new LongWritable(tail));
        }
    }

    public static class EdgePartitioner extends Partitioner<LongWritable,LongWritable> {
        @Override
        public int getPartition(LongWritable longWritable, LongWritable longWritable2, int i) {
            return (int)(longWritable.get() % i);
        }
    }


    public static class EdgeReducer  extends Reducer<LongWritable,LongWritable, LongWritable, LongWritable> {

        @Override
        public void reduce(LongWritable tail, Iterable<LongWritable> valueSet,
                           Context context) throws IOException, InterruptedException {
            int counter = 0;
            Iterator<LongWritable> iterator = valueSet.iterator();
            while(iterator.hasNext()) {
                iterator.next();
                counter+=1;
            }
            context.write(tail,new LongWritable(counter));
        }
    }

    public static class DegreeMapper  extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {


        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(value, key);
        }
    }

    public static class DegreePartitioner extends Partitioner<LongWritable,LongWritable> {
        @Override
        public int getPartition(LongWritable longWritable, LongWritable longWritable2, int i) {
            return (int)(longWritable.get() % i);
        }
    }

    public static class DegreeReducer  extends Reducer<LongWritable,LongWritable, LongWritable, LongWritable> {

        private OutputStream outputStream  = null;

        @Override
        public void setup(Context context) {
            try {
                Configuration conf = context.getConfiguration();
                String dataDir = conf.get("ldbc.snb.bteronh.serializer.dataDir");
                int reducerId = context.getTaskAttemptID().getTaskID().getId();

                FileSystem fs = FileSystem.get(conf);
                outputStream = fs.create(new Path(dataDir + "/degree_distribution_"+ reducerId), true, 131072);
            } catch (IOException e) {
                System.err.println(e.getMessage());
                System.exit(-1);
            }

        }

        @Override
        public void reduce(LongWritable tail, Iterable<LongWritable> valueSet,
                           Context context) throws IOException, InterruptedException {
            int counter = 0;
            Iterator<LongWritable> iterator = valueSet.iterator();
            while(iterator.hasNext()) {
                iterator.next();
                counter+=1;
            }
            String str = new String(tail+" "+counter+"\n");
            outputStream.write(str.getBytes());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputStream.close();
        }
    }

    static private Configuration conf = null;

    public static void main(String [] args) {

        Arguments arguments = new Arguments();
        new JCommander(arguments,args);

        conf = new Configuration();
        conf.set("ldbc.snb.bteronh.serializer.outputDir",arguments.outputfolder);
        conf.set("ldbc.snb.bteronh.serializer.dataDir",conf.get("ldbc.snb.bteronh.serializer.outputDir")+"/distribution/data");
        conf.set("ldbc.snb.bteronh.serializer.hadoopDir",conf.get("ldbc.snb.bteronh.serializer.outputDir")+"/distribution/hadoop");

        try {
            FileSystem dfs = FileSystem.get(conf);
            dfs.delete(new Path(conf.get("ldbc.snb.bteronh.serializer.hadoopDir")), true);
            dfs.delete(new Path(conf.get("ldbc.snb.bteronh.serializer.dataDir")), true);
            dfs.mkdirs(new Path(conf.get("ldbc.snb.bteronh.serializer.outputDir")));

            String hadoopDir = new String( conf.get("ldbc.snb.bteronh.serializer.hadoopDir"));
            Path paths [] = new Path[arguments.inputfiles.size()];
            int index = 0;
            for(String file : arguments.inputfiles) {
                Path path = new Path(file);
                paths[index] = path;
                index+=1;
            }

            System.out.println("Processing edges");
            Job job = Job.getInstance(conf, "Edge processing job");
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(LongWritable.class);
            job.setJarByClass(EdgeMapper.class);
            job.setMapperClass(EdgeMapper.class);
            job.setReducerClass(EdgeReducer.class);
            job.setNumReduceTasks(arguments.numthreads);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setPartitionerClass(EdgePartitioner.class);
            FileInputFormat.setInputPaths(job, paths);
            FileOutputFormat.setOutputPath(job, new Path(hadoopDir+"/degrees"));
            try {
                job.waitForCompletion(true);
            } catch(InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }

            System.out.println("Processing degrees");
            job = Job.getInstance(conf, "Degree processing job");
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(LongWritable.class);
            job.setJarByClass(DegreeMapper.class);
            job.setMapperClass(DegreeMapper.class);
            job.setReducerClass(DegreeReducer.class);
            job.setNumReduceTasks(arguments.numthreads);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setPartitionerClass(DegreePartitioner.class);
            FileInputFormat.setInputPaths(job, new Path(hadoopDir+"/degrees"));
            FileOutputFormat.setOutputPath(job, new Path(hadoopDir+"/blackhole"));
            try {
                job.waitForCompletion(true);
            } catch(InterruptedException e) {
                e.printStackTrace();
                System.exit(1);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
            dfs.delete(new Path(hadoopDir+"/blackhole"),true);

        } catch(IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
