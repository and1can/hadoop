import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.security.NoSuchAlgorithmException;
import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

public class TotalFailMapper extends Mapper<LongWritable, Text, WTRKey,
                                            RequestReplyMatch> {

    private MessageDigest messageDigest;
    @Override
    public void setup(Context ctxt) throws IOException, InterruptedException{
        // You probably need to do the same setup here you did
        // with the QFD writer
        super.setup(ctxt);
		try{
			messageDigest = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("SHA-1 algorithm not available");
		}
		messageDigest.update(HashUtils.SALT.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void map(LongWritable lineNo, Text line, Context ctxt)
            throws IOException, InterruptedException {

        // The value in the input for the key/value pair is a Tor IP.
        // You need to then query that IP's source QFD to get
        // all cookies from that IP,
        // query the cookie QFDs to get all associated requests
        // which are by those cookies, and store them in a torusers QFD

    	System.out.println("tor user");
		System.out.println(line);
		MessageDigest md = HashUtils.cloneMessageDigest(messageDigest);
		md.update(line.toString().getBytes(StandardCharsets.UTF_8));
		byte[] hash = md.digest();
		byte[] hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
	    String hashString = DatatypeConverter.printHexBinary(hashBytes);
		WTRKey key = new WTRKey("srcIP", hashString);
		System.out.println("key is ");
		System.out.println(key);	
		System.out.println("hashCode ");
		System.out.println(key.hashCode());
		System.out.println("hashBytes ");
		System.out.println(key.getHashBytes());
		QueryFocusedDataSet e = null;
		try
		{
			FileInputStream fileIn = new FileInputStream("qfds/srcIP/srcIP_" + key.getHashBytes());
			ObjectInputStream in = new ObjectInputStream(fileIn);
		    e = (QueryFocusedDataSet) in.readObject();
			in.close();
			fileIn.close();
		} catch(IOException i) 
		{ 
			i.printStackTrace();
			return;
		} catch(ClassNotFoundException c)
		{ 
			System.out.println("qfds not found");
			c.printStackTrace();
			return;
		}
		System.out.println("qfds");
		System.out.println(e.getName());
		System.out.println(e.getMatches());	
		System.out.println("cookies are ");
		HashSet<WebTrafficRecord> replies = new HashSet<>();
		for (RequestReplyMatch i : e.getMatches()){
			replies.add(new WebTrafficRecord(i.getRequest())); 
			WebTrafficRecord cookie = i.getRequest();
			md = HashUtils.cloneMessageDigest(messageDigest);
			md.update(cookie.getCookie().getBytes(StandardCharsets.UTF_8));
		    hash = md.digest();
			hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
			hashString = DatatypeConverter.printHexBinary(hashBytes);
			WTRKey key1 = new WTRKey("cookie", hashString);
			QueryFocusedDataSet j = null;
			try
			{
				FileInputStream fileIn = new FileInputStream("qfds/cookie/cookie_" + key1.getHashBytes());
				ObjectInputStream in = new ObjectInputStream(fileIn);
				j = (QueryFocusedDataSet) in.readObject();
				in.close();
				fileIn.close();
			} catch(IOException k)
			{
				k.printStackTrace();
				return;
			} catch(ClassNotFoundException l)
			{
				System.out.println("qfds not found");
				l.printStackTrace();
				return;
			}
			for (RequestReplyMatch m : j.getMatches()){
				System.out.println("create toruser");
				md = HashUtils.cloneMessageDigest(messageDigest);
				md.update(m.getReply().getUserName().getBytes(StandardCharsets.UTF_8));
				hash = md.digest();
				hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
				hashString = DatatypeConverter.printHexBinary(hashBytes);
				WTRKey k = new WTRKey("torusers", hashString);
				ctxt.write(k, m);
			}
		}
		System.out.println(replies);
	}
}
