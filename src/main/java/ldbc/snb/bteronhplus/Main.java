package ldbc.snb.bteronhplus;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import ldbc.snb.bteronhplus.algorithms.Partitioning;
import ldbc.snb.bteronhplus.algorithms.SuperNodeUtils;
import ldbc.snb.bteronhplus.structures.*;

import java.io.FileWriter;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class Main {

    public static class Arguments {

        @Parameter(names = {"-d", "--degrees"}, description = "The file with the degrees", required = true)
        public String degreesFile;

        @Parameter(names = {"-cc", "--clustering"}, description = "The file with the clustering coefficient " +
                "distributions per degree", required = true)
        public String ccsFile;

        @Parameter(names = {"-c", "--communities"}, description = "The file with the community sizes", required = true)
        public String communitiesfile;

        @Parameter(names = {"-o", "--output"}, description = "The output file", required = true)
        public String outputFileName;

        @Parameter(names = {"-p", "--density"}, description = "The community densities file", required = true)
        public String densityFileName;

        @Parameter(names = {"-s", "--size"}, description = "The size of the graph", required = true)
        public int graphSize;

        @Parameter(names = {"-cs", "--csprefix"}, description = "The community structure file name prefix", required =
                true)
        public String communityStructureFileNamePrefix;

        @Parameter(names = {"-l", "--levels"}, description = "The number of community structure levels " )
        public int levels = 2;
    
        @Parameter(names = {"-m", "--modules"}, description = "The modules file prefix", required = true)
        public String modulesPrefix;


    }

    public static void main(String[] args) throws Exception {

        Arguments arguments = new Arguments();
        new JCommander(arguments, args);
    
        System.out.println("Degree file: "+arguments.degreesFile);
        System.out.println("CCS file: "+arguments.ccsFile);
        System.out.println("Communities file: "+arguments.communitiesfile);
        System.out.println("Density file: "+arguments.densityFileName);
        System.out.println("Modules prefix: "+arguments.modulesPrefix);

        GraphStats graphStats = new GraphStats(arguments.degreesFile,
                                               arguments.ccsFile,
                                               arguments.communitiesfile,
                                               arguments.densityFileName);


        LinkedList<BlockModel> blockModelHierarchy = new LinkedList<BlockModel>();
        for(int i = arguments.levels-1; i > 0 ; --i) {
            byte[] byteArray = Files.readAllBytes(Paths.get(arguments.communityStructureFileNamePrefix+i));
            String blockModelData = new String(byteArray);
            byteArray = Files.readAllBytes(Paths.get(arguments.communityStructureFileNamePrefix+i+".children"));
            String childrenData = new String(byteArray);
            BlockModel blockModel = new BlockModel(blockModelData, childrenData);
            blockModelHierarchy.add(blockModel);

        }

        if(blockModelHierarchy.size() > 0) {
            BlockModel first = blockModelHierarchy.getFirst();
            Set<Long> blockIds = first.getEntries().keySet();
            Map<Long, List<Long>> children = new HashMap<Long, List<Long>>();
            children.put(0L,new ArrayList<Long>(blockIds));
            BlockModel root = BlockModel.identity();
            root.setChildren(children);
            blockModelHierarchy.addFirst(root);
        } else {
           blockModelHierarchy.addLast(BlockModel.identity());
        }
    
        Random random = new Random();
        random.setSeed(12345L);

        //CommunityStreamer communityStreamer = new CommunityStreamer(graphStats);
        //CorePeripheryCommunityStreamer communityStreamer = new CorePeripheryCommunityStreamer(graphStats,random);
        RealCommunityStreamer communityStreamer = new RealCommunityStreamer(graphStats, arguments.modulesPrefix+
            "communities", arguments.modulesPrefix+"degreemap", random);
        Map<Long,SuperNodeCluster>  partition = Partitioning.partition(blockModelHierarchy.getLast(),
                                                                       communityStreamer,
                                                                       arguments.graphSize);
        
        System.out.println("Building Hiearchy");
        SuperNodeCluster root = SuperNodeUtils.buildSuperNodeClusterHierarchy(blockModelHierarchy,
                                                                              partition);
    
        System.out.println("Writting community edges");
        FileWriter outputFile = new FileWriter(arguments.outputFileName);
    

        //root.dumpInternalEdges(outputFile,0);
        long totalDegree = (long)(root.getInternalDegree() + root.getExternalDegree());
        totalDegree /=2;
    
        System.out.println("Writting external edges");
        System.out.println("Total Number of expected edges: "+totalDegree);
        int numExternalEdges = 0;
        root.sampleEdges(outputFile,random,totalDegree,0);

        outputFile.close();
    }
}
