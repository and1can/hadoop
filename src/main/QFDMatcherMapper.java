import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class QFDMatcherMapper extends Mapper<LongWritable, Text,
				      IntWritable, WebTrafficRecord> {

    @Override
    public void map(LongWritable lineNo, Text line, Context ctxt)
	throws IOException, InterruptedException {
        // Inputs come on lines of text that can be parsed
        // as WebTrafficRecord, your key should be such that all
        // records with the same source IP/source port/dest IP/dest port
        // are the same so they always go to the same reducer...
		WebTrafficRecord value = WebTrafficRecord.parseFromLine(line.toString());
//		System.out.println(value.hashCode());
//		System.err.println(" above is value and then the key QFDMatcherMapper");
    	ctxt.write(new IntWritable(value.matchHashCode()), value);
//		ctxt.write(new IntWritable(value.hashCode()), null);
	}
}
