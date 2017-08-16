import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfileMapper extends Mapper<LongWritable,Text,IntWritable,Text>  {
	
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
		
		
		String line = value.toString();
		
		
		String[] columns = line.split(",");
		
			if(columns.length<=8){
				for(int i=0;i<columns.length;i++){
					context.write(new IntWritable(i),new Text((columns[i])));
				
			}
			}
			
		
      
		
		
		
	}

}
