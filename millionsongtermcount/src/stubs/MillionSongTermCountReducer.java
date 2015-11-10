package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MillionSongTermCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  IntWritable totalCount = new IntWritable();
  
  @Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		totalCount.set(0);
		
		for (IntWritable value : values) {
			
			//sum up all the words to determine total count
			totalCount.set(totalCount.get()+value.get());
		}
		
		context.write(key, totalCount);
	}
}