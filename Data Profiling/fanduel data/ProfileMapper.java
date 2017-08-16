import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfileMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	 
		  String line = value.toString();
		  String[] bits = line.split(",");
		  for(int i = 0; i<bits.length; i++){
			  context.write(new IntWritable(i), new Text(bits[i]));
		  }
	  
	 

  }
}
