import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class NFLMapper extends Mapper<LongWritable,Text,Text,Text>  {
	
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
		
		
		String line = value.toString();
        line = line.replaceAll("\"", "");
		String[] columns = line.split(",");
		
		if(String.valueOf(columns[1]).equals("Year")){
			String rest = "";
			for(int i=2;i<columns.length;i++){
				if(i==5){
					rest = rest + "," + "Temperature,Forecast";
				}
				else{
					rest = rest+  "," + columns[i];
				}
			}
				
			context.write(new Text(" "+columns[1]),new Text(rest) );
		}
		
		else{
			String rest = "";
			if(String.valueOf(columns[5]).contains("f")){
				String[] tempsplit = columns[5].split("f");
				for(int i=2;i<columns.length;i++){
					if(i==5){
						for(String j : tempsplit){
							j = j.replaceAll(" ", "");
							rest = rest +  "," + j + " ";
						}
						
					}
					else{
						rest = rest + "," + columns[i];
					}
					
				}
				context.write(new Text(columns[1]), new Text(rest));
			}
			else{
				String rest2 = "";
				for(int i=2;i<columns.length;i++){
					if(i==5){
						rest2 = rest2 + "," + "DOME,";
						
					}
					else{
						rest2 = rest2 + "," + columns[i];
					}
					
				}
				context.write(new Text(columns[1]), new Text(rest2));
			}
			
			
		}
		
		
		
		
	}

}
