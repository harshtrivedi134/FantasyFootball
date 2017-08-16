import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class newReducer extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  String game = key.toString();
	  String[] cols = {"Pass Yds", "Pass TDs", "Ints Thrown", "Rush Yds", "Rush TDs", "Reception", "Receiving Yds", "Receiving TDs", "Sacks", "Ints Caught", "FF", "FR", "Points"};
	  for(Text value:values){
		  String tmp = value.toString();
		  String[] info = tmp.split(" ");
		  String player = info[0];
		  int col = Integer.parseInt(info[1]);
		  String column = cols[col-7];
		  double val = Double.parseDouble(info[2]);
		  if(val != 0){
			  String write = player+" - "+column+": "+val;
			  context.write(new Text(game), new Text(write));
		  }
	  }
  }
}
