import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class SMAReducer extends
	Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>{
	
	private IntWritable out = new IntWritable();
	private FloatWritable result = new FloatWritable();
	
	public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
		       throws IOException, InterruptedException {
		float sum = 0;
		int count = 0;
		
		for(FloatWritable val: values){
			sum += val.get();
			count++;
		}
		sum = sum/count;
		result.set(sum);
		out = key;
		
		context.write(out, result);
	}
}
