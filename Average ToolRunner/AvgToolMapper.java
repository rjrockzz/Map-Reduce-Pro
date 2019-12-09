package AverageToolRunner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class AvgToolMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	boolean caseSensitive = false;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
 
		for (String word : line.split("\\W+")) {
			if (word.length() > 0) {

				String letter;

				if (caseSensitive)
					letter = word.substring(0, 1);
				else
					letter = word.substring(0, 1).toLowerCase();

					context.write(new Text(letter), new IntWritable(word.length()));
			}
		}
	}

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		caseSensitive = conf.getBoolean("caseSensitive", false);
	}
}