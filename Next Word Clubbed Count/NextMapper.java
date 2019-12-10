package CountWordNext;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;

public class NextMapper extends Mapper<LongWritable,Text, Text, IntWritable> 
{
	@Override
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException
	{
		String line = value.toString();
		String secondword=new String();
        String firstword=new String();

        StringTokenizer itr = new StringTokenizer(line);

//      Initializing the firstword and the secondword.
        if (itr.hasMoreTokens()) {
            firstword = itr.nextToken();
            secondword = firstword;
        }
        
        
//      At the start of each loop the secondword is put into the firstword and concatenated with it's subsequent word.
        while (itr.hasMoreTokens())
        {
            firstword = secondword;
            secondword = itr.nextToken();;
            firstword = firstword+" "+secondword;

            Text outputKey = new Text(firstword.toLowerCase());
            IntWritable outputValue = new IntWritable(1);
            context.write(outputKey, outputValue);
            
//      At the end of each line it will only take the last word as key.
            if (!itr.hasMoreTokens())
            {
                outputKey.set(secondword.toLowerCase());
                context.write(outputKey, outputValue);
            }
        }
    }
}