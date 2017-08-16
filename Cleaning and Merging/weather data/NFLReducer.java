import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class NFLReducer extends Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
		
		String temp="";
		
		
		for(Text value : values){			
			context.write(new Text(key.toString().replaceAll(" ", "")),new Text(value.toString()));	
			
			}
			
		
				
		}
			
		
		
	}

