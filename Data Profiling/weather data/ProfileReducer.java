import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.*;
import java.math.*;

public class ProfileReducer extends Reducer<IntWritable,Text,Text,Text> {
	
	public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
		
		String[] columns = {"Year","Week","Away","Home","Temperature","Forecast","Extended Forecast","Wind"};
		String temp="";
		int k = key.get();
		if(k==4){
			int maxtemp=0,avgtemp=0,sumtemp=0,count=0,mintemp=99999;
			for(Text value : values){
				    if(value.toString().contains("DOME") || value.toString().contains("/") || value.toString().equals("Temperature")){
				    	
				    }
				    else{
				    	sumtemp += Integer.parseInt(value.toString());
				    	maxtemp = Math.max(maxtemp,Integer.parseInt(value.toString()));
				    	mintemp = Math.min(mintemp,Integer.parseInt(value.toString()));
				    	count++;
				    }
				    
				       
				
				
			}
			avgtemp=sumtemp/count;
			String out =  " Type:Integer" + " Average Value: " + avgtemp + " Max Value: " + maxtemp + " Min Value: " + mintemp + " Temperature Range: " + mintemp + "-" + maxtemp;
			context.write(new Text(columns[k]), new Text(out));
		}
		
		else{
			int maxlen=0,avglen=0,sumlen=0,count=0,minlen=99999;
			String out;
			for(Text value : values){
				temp = value.toString();
				if(value!=null){
					sumlen += (k==0?temp.length()-1:temp.length());
					maxlen = (k==0?Math.max(maxlen, temp.length()-1):Math.max(maxlen, temp.length()));
					minlen = (k==0?Math.min(minlen, temp.length()-1):Math.min(minlen, temp.length()));
					count++;
				}
				
				
			}
			avglen = sumlen/count;
			if(k==0){
				 out =  " Type:String" + " Average Length: " + avglen + " Max Length: " + maxlen + " Min Length: " + minlen + " Value Range: \"2009-2016\"" ;
			}
			else
			out =  " Type:String" + " Average Length: " + avglen + " Max Length: " + maxlen + " Min Length: " + minlen + " Length Range: " + minlen +"-" + maxlen;
			context.write(new Text(columns[k]), new Text(out));
		}
		
			
		
				
		}
			
		
		
	}

