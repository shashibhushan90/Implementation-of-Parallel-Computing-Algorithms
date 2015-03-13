import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class SMAMapper extends
Mapper<Object, Text, IntWritable, FloatWritable> {
	
	IntWritable mykey = new IntWritable();
	FloatWritable result = new FloatWritable(); 
	int counter = 0;
	int window_length = 4;
		
	public void map(Object key, Text value, Context context)
			 throws IOException, InterruptedException {
		String ArrLine[] = value.toString().split(",");
		
		counter++;
		//if (counter > 0) {
			if(counter < window_length){
					if(counter == 1){
						mykey.set(1);
						result.set(Float.parseFloat(ArrLine[4]));
						context.write(mykey, result );
					}
					else if(counter > 1) {
						for(int i=1; i<=(counter); i++){
							mykey.set(i);
							result.set(Float.parseFloat(ArrLine[4]));
							context.write(mykey, result );
						}
					}
				}
			else if(counter >= window_length){
				
				for(int i=((counter-window_length)+1); i <= (counter); i++){
					mykey.set(i);
					result.set(Float.parseFloat(ArrLine[4]));
					context.write(mykey, result );
					}
			}
			
			
		//}
	}
}
