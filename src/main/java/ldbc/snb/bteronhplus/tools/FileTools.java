package ldbc.snb.bteronhplus.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;

public class FileTools {

    public static String isHDFSPath(String fileName) {
        if (fileName.startsWith("hdfs://")) {
            return fileName.substring(7);
        }
        return null;
    }

    public static String isLocalPath(String fileName) {
        if (fileName.startsWith("file://")) {
            return fileName.substring(7);
        }
        return null;
    }

    public static BufferedReader getFile(String fileName, Configuration conf) throws IOException {
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

    static public <T,S> void serializeHashMap (HashMap<T,S> map, String path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fs.create(new Path(path)));
        objectOutputStream.writeObject(map);
        objectOutputStream.close();
    }

    static public <T,S> HashMap<T,S> deserializeHashMap (String path, Configuration conf) throws
            IOException {
        FileSystem fs = FileSystem.get(conf);
        ObjectInputStream objectInputStream = new ObjectInputStream(fs.open(new Path(path)));
        try {
            HashMap<T,S> map =  (HashMap<T, S>) objectInputStream.readObject();
            objectInputStream.close();
            return map;
        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            System.exit(1);
        }
        objectInputStream.close();
        return null;
    }

    static public <T>  void serializeObjectArray( T[] array, String path, Configuration conf ) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fs.create(new Path(path)));
        objectOutputStream.writeObject(array);
        objectOutputStream.close();

    }

    static public <T>  T[] deserializeObjectArray( String path, Configuration conf ) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        ObjectInputStream objectInputStream = new ObjectInputStream(fs.open(new Path(path)));
        try {
            T array [] = (T[])objectInputStream.readObject();
            objectInputStream.close();
            return array;
        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            System.exit(1);
        }
        objectInputStream.close();
        return null;

    }

    static public <T>  void serializeObject( T object, String path, Configuration conf ) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fs.create(new Path(path)));
        objectOutputStream.writeObject(object);
        objectOutputStream.close();

    }

    static public <T>  T deserializeObject( String path, Configuration conf ) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        ObjectInputStream objectInputStream = new ObjectInputStream(fs.open(new Path(path)));
        try {
            T object = (T)objectInputStream.readObject();
            objectInputStream.close();
            return object;
        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            System.exit(1);
        }
        objectInputStream.close();
        return null;

    }


}
