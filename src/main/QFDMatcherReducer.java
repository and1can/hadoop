import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import java.util.*;
import java.io.IOException;

public class QFDMatcherReducer extends Reducer<IntWritable, WebTrafficRecord, RequestReplyMatch, NullWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<WebTrafficRecord> values,
                       Context ctxt) throws IOException, InterruptedException {

        // The input is a set of WebTrafficRecords for each key,
        // the output should be the WebTrafficRecords for
        // all cases where the request and reply are matched
        // as having the same
        // Source IP/Source Port/Destination IP/Destination Port
        // and have occured within a 10 second window on the timestamp.

        // One thing to really remember, the Iterable element passed
        // from hadoop are designed as READ ONCE data, you will
        // probably want to copy that to some other data structure if
        // you want to iterate mutliple times over the data.

		List<WebTrafficRecord> valueList = new ArrayList<>();
		// store the values into ArrayList so that we can compare all possible combinations
		for (WebTrafficRecord wtr : values) {
			valueList.add(new WebTrafficRecord(wtr));
		}
//		System.out.println(valueList.size());
//		System.out.println("iterate via 'for loop'");
		// compare all possible combinations to find a reply match pair : also happens within 10 seconds
		for (WebTrafficRecord wtr : valueList) {
			for (WebTrafficRecord w : valueList) {
//				System.out.println(wtr);
//				System.out.println(w);
				if (wtr.equals(w)) {
//				if (wtr.tupleMatches(w)) {
//					System.out.println("two traffic records are same, do not match");
				}
				//  need this in case we have a null for either traffic record
				else if ((wtr != null) && (w != null)) {
					if (wtr.matchHashCode() == w.matchHashCode()) {
//					System.out.println("We have a match");
						if ((Math.abs(w.getTimestamp() - wtr.getTimestamp())) <= 10) {
//						System.out.println("time stamp is within 10 s, make request reply match");
							if (w.getUserName() == null) {
//							System.out.println("second is the request");
								ctxt.write(new RequestReplyMatch(w, wtr), NullWritable.get()); 		
//								return;
							} else {
//							System.out.println("first is the request");
						    	ctxt.write(new RequestReplyMatch(wtr, w), NullWritable.get());	
//								return;
							}		
						}
					}
//				System.out.println("compare the above pair"); 
				}
			}
		}
//		System.out.println("Implement QFDMatcherReducer");
		// ctxt.write should be RequestReplyMatch and a NullWriteable
    }
}
