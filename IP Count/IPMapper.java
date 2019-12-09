package IPCount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


    public class IPMapper extends Mapper<Object, Text, Text, IntWritable>{
    
	@Override
	
	public void map(Object key, Text value, Context context)
	    throws IOException, InterruptedException {
		
		String line = value.toString();
		String IPADDRESS_PATTERN = 
		        "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

		Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
		Matcher matcher = pattern.matcher(line);
		String ip = new String();
		if (matcher.find())
		    ip = matcher.group().toString();
		int len = ip.length();
		if(len>0)
		{
			context.write(new Text(ip),new IntWritable(1));
	}
	}
}
