import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfileReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

  @Override
  public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  boolean isNumeric = false;
	  for(Text value:values){ 
		  String temp = value.toString();
		  try{
			  int y = Integer.parseInt(temp);
			  isNumeric = true; // only need to check one
			  break;
		  }catch(Exception e){
			  isNumeric = false;
		  }
	  }
	  if(isNumeric){
		  double min = Integer.MAX_VALUE;
		  double max = 0; //nothing is less than that
		  for(Text value: values){
			  String temp = value.toString();
			  int tmp = Integer.parseInt(temp);
			  min = Math.min(min, tmp);
			  max = Math.max(max, tmp);
		  }
		  context.write(key, new Text("Numeric"));//flags that the column is numeric
		  context.write(key, new Text(Double.toString(min)));
		  context.write(key, new Text(Double.toString(max)));
	  }else{
		  double maxlength = 0;
		  double minlength = Integer.MAX_VALUE;
		  for(Text value: values){
			  String temp = value.toString();
			  maxlength = Math.max(maxlength, temp.length());
			  minlength = Math.min(minlength, temp.length());
		  }
		  context.write(key, new Text("String")); //flags that the column is a string
		  context.write(key, new Text(Double.toString(minlength)));
		  context.write(key, new Text(Double.toString(maxlength)));
	  

  }
}
}