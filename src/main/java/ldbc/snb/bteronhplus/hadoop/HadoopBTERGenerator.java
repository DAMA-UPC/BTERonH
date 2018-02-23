package ldbc.snb.bteronhplus.hadoop;

import ldbc.snb.bteronhplus.algorithms.BTER;
import ldbc.snb.bteronhplus.structures.BTERStats;
import ldbc.snb.bteronhplus.structures.Edge;
import ldbc.snb.bteronhplus.tools.FileTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
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

            int threadId = Integer.parseInt(value.toString());

            Configuration conf = context.getConfiguration();
            String bterStatsFile = conf.get("ldbc.snb.bteronh.generator.bterstats");
            int numThreads = conf.getInt("ldbc.snb.bteronh.generator.numThreads",1);
            int seed = conf.getInt("ldbc.snb.bteronh.generator.seed",0);

            BTERStats stats = FileTools.deserializeObject(bterStatsFile,conf);

            long offset = conf.getLong("offset",0);
            int communityId = conf.getInt("communityId",0);

            long totalWeight = stats.getWeightPhase1()+stats.getWeightPhase2();
            long blockSize =  totalWeight / numThreads;
            if(threadId == 0) {
                long residual = totalWeight % numThreads;
                blockSize += residual;
            }
            System.out.println("Edges to generate at reducer "+threadId+" for parition "+communityId+":"+blockSize);

            System.out.println("Mapper "+threadId+": Generating "+blockSize+" edges out of "+totalWeight);
            Random random = new Random();
            random.setSeed(seed+threadId);
            for(long i = 0; i < blockSize; ++i) {
                Edge edge = BTER.BTERSample(stats,random,offset);
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
                //String str = new String(tail + " " + head + "\n");
                context.write(tail,new LongWritable(head));
            }
        }
    }

}
