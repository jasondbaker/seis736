package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MillionSongTCSMappper extends Mapper<Text, Text, IntWritable, Text> {
 
	IntWritable intwritable = new IntWritable();
	
	@Override
  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
    	
	// swap the key and the value
	intwritable.set(Integer.parseInt(value.toString()));
	context.write(intwritable, key);
	
  }
}