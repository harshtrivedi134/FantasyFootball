import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class ProfileDriver {
	
	public static void main(String args[]) throws IOException,InterruptedException,ClassNotFoundException {
		
		if(args.length != 2){
			System.err.println("Provide 2 valid commandline arguments in the format <input Path> <output Path>");
			System.exit(-1);
			
		}
		
		//conf.set("mapreduce.output.textoutputformat.separator", ",");
		
		
		
		
		
		Job job = new Job();
		
		
		job.setJarByClass(ProfileDriver.class);
		job.setJobName("Data Profile");		
		
		
		FileInputFormat.addInputPath(job , new Path(args[0]));
		FileOutputFormat.setOutputPath(job , new Path(args[1]));
		
		
		job.setMapperClass(ProfileMapper.class);
		job.setReducerClass(ProfileReducer.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}