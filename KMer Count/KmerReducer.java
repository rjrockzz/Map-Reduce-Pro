package KMER;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text nineMer, Iterable<IntWritable> values, Context con)
			throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable value : values) {

			sum += value.get();

		}

		con.write(nineMer, new IntWritable(sum));

	}

}