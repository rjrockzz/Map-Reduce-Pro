package Average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


    public class AvgMapper extends Mapper<Object, Text, Text, IntWritable>{
    
	private Text firstCharacter = new Text();
	private static final IntWritable length = new IntWritable(1);
    
	@Override
	
	public void map(Object key, Text value, Context context)
	    throws IOException, InterruptedException {
		
		String line = value.toString();
		
		for (String word : line.split("\\W+")) {
		      if (word.length() > 0) {
		        
		    	  firstCharacter.set(word.substring(0,1));
		    	  length.set(word.length());
		        context.write(firstCharacter, length);	//public void write(KEYOUT key,VALUEOUT value)
		      }
		}
	}
}