package Average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AvgReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	private IntWritable results = new IntWritable();
	 
	@Override	
	 public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
     int sum = 0;
     int total = 0;
		for (IntWritable value : values) {
			total++;
			sum += value.get();
		}
		int average = sum / total;
		results.set(average);
		context.write(key, results);
	}	
}