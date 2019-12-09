package KMER;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KmerDriver {
	
	public static void main(String [] args) throws Exception

	{

	Configuration c=new Configuration();

	String[] files=new GenericOptionsParser(c,args).getRemainingArgs();

	Path input=new Path(files[0]);

	Path output=new Path(files[1]);
	
	@SuppressWarnings("deprecation")
	Job j=new Job(c,"kmer");
	
	//Job j=new Job();

	j.setJarByClass(KmerDriver.class);

	j.setMapperClass(KmerMapper.class);

	j.setReducerClass(KmerReducer.class);
	
	j.setMapOutputKeyClass(Text.class);
	
	j.setMapOutputValueClass(IntWritable.class);

	j.setOutputKeyClass(Text.class);

	j.setOutputValueClass(IntWritable.class);

	FileInputFormat.addInputPath(j, input);

	FileOutputFormat.setOutputPath(j, output);

	System.exit(j.waitForCompletion(true)?0:1);

	}

}