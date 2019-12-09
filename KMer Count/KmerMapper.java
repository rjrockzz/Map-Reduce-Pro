package KMER;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context con)
			throws IOException, InterruptedException {

		String line = value.toString();
		if (line.contains(">gi") && line.length() < 3) {
			
			return;
		}
			for (int merCounter = 0; merCounter < line.length() - 3; merCounter++)
			{
				Text outputKey = new Text(line.substring(merCounter, merCounter + 3).trim());
				IntWritable outputValue = new IntWritable(1);
				con.write(outputKey, outputValue);
			}
		}
	}