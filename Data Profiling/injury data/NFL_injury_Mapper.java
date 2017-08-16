import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class newMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final int MISSING =9999;
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	  String line = value.toString();
	  String[] bits = line.split(",");
	  String  ID = bits[1];//unique identifier
	  
	  String game = "16"+bits[4];//year, week
	  if(bits[5].compareTo(bits[6]) < 0){//teams, in alphabetical order
		  game += bits[5];
		  game += bits[6];
	  }else{
		  game += bits[6];
		  game += bits[5];
	  }
	  
	  for(int i = 7; i<bits.length; i++){
		  String towrite = ID+" "+i+" "+bits[i]; //player, column, value
		  context.write(new Text(game), new Text(towrite));//we'll eventually want to sort by game
	  }
	 
  }
}
