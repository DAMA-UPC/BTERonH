package ldbc.snb.bteronh;

import com.beust.jcommander.JCommander;
import ldbc.snb.bteronh.hadoop.HadoopBTERGenerator;
import ldbc.snb.bteronh.structures.Arguments;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class BTERMain {
    public static void main(String [] args) {

        System.out.println("Generating edges");
        Configuration conf = new Configuration();
        conf.setInt("ldbc.snb.bteronh.generator.numThreads",1);
        conf.setInt("ldbc.snb.bteronh.generator.numNodes",10000);
        conf.setInt("ldbc.snb.bteronh.generator.seed",12323540);
        conf.set("ldbc.snb.bteronh.serializer.outputDir","./");
        conf.set("ldbc.snb.bteronh.generator.degreeSequence","src/main/resources/degreeSequences/dblp");
        conf.set("ldbc.snb.bteronh.generator.ccPerDegree","src/main/resources/CCs/dblp");


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

        conf.set("ldbc.snb.bteronh.serializer.dataDir",conf.get("ldbc.snb.bteronh.serializer.outputDir")+"/data");
        conf.set("ldbc.snb.bteronh.serializer.hadoopDir",conf.get("ldbc.snb.bteronh.serializer.outputDir")+"/hadoop");


        try {
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(conf.get("ldbc.snb.bteronh.serializer.hadoopDir")), true);
        dfs.delete(new Path(conf.get("ldbc.snb.bteronh.serializer.dataDir")), true);

            System.out.println("Starting execution");
            long start = System.currentTimeMillis();
            HadoopBTERGenerator bterGenerator = new HadoopBTERGenerator(conf);
            bterGenerator.run();
            long end = System.currentTimeMillis();
            System.out.println("Execution time: "+(end-start)/1000.0+" second");
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}
