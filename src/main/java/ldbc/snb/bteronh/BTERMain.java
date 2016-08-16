package ldbc.snb.bteronh;

import ldbc.snb.bteronh.hadoop.HadoopBTERGenerator;
import org.apache.hadoop.conf.Configuration;

class BTERMain {
    public static void main(String [] args) {



        System.out.println("Generating edges");
        Configuration conf = new Configuration();
        conf.setInt("ldbc.snb.bteronh.generator.numThreads",2);
        conf.setInt("ldbc.snb.bteronh.generator.numNodes",317080);
        conf.set("ldbc.snb.bteronh.generator.graph","dblp");
        conf.setInt("ldbc.snb.bteronh.generator.seed",12323540);
        conf.set("ldbc.snb.bteronh.serializer.hadoopDir","./hadoop");
        conf.set("ldbc.snb.bteronh.serializer.outputDir","./data");
        HadoopBTERGenerator bterGenerator = new HadoopBTERGenerator(conf);
        try {
            bterGenerator.run();
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}
