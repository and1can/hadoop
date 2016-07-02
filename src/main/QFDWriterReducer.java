import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import java.util.*;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.io.*;
import org.apache.hadoop.fs.FSDataOutputStream;

public class QFDWriterReducer extends Reducer<WTRKey, RequestReplyMatch, NullWritable, NullWritable> {

    @Override
    public void reduce(WTRKey key, Iterable<RequestReplyMatch> values,
                       Context ctxt) throws IOException, InterruptedException {

        // The input will be a WTR key and a set of matches.

        // You will want to open the file named
        // "qfds/key.getName()/key.getName()_key.getHashBytes()"
        // using the FileSystem interface for Hadoop.

        // EG, if the key's name is srcIP and the hash is 2BBB,
        // the filename should be qfds/srcIP/srcIP_2BBB

        // Some useful functionality:

        // FileSystem.get(ctxt.getConfiguration())
        // gets the interface to the filesysstem
        // new Path(filename) gives a path specification
        // hdfs.create(path, true) will create an
        // output stream pointing to that file

		HashSet<RequestReplyMatch> matchList = new HashSet<>();
    	for (RequestReplyMatch r : values) {
			matchList.add(new RequestReplyMatch(r));
		}
//		System.out.println("set made ");
//		System.out.println(matchList);
		String a = key.getName();
		String b = key.getHashBytes();
//		System.out.println("path is ");
		String path = "qfds" + "/" + a + "/" + a + "_" + b;
//		System.out.println(path);
		FileSystem hdfs = FileSystem.get(ctxt.getConfiguration());
		Path p = new Path(path);
		FSDataOutputStream fileOut = hdfs.create(p, true);
		QueryFocusedDataSet qfds = new QueryFocusedDataSet(key.getName(), key.getHashBytes(), matchList);
		
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(qfds);
		fileOut.close();
		out.close();
	}
}
