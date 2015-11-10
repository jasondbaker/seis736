package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MillionSongTitleCountByTermReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  private MultipleOutputs<Text, IntWritable> multipleOutputs;
	
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
	  
	  multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
  }
  
  IntWritable totalCount = new IntWritable();
  Text newKey = new Text();
  
  @Override
  
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		totalCount.set(0);
		
		for (IntWritable value : values) {
		    
			// sum up the values to get a totalcount
			totalCount.set(totalCount.get()+value.get());
		}
		
		// pull out the word from the combined key
		String[] keys = key.toString().split(",");
		newKey.set(keys[1]);
		
		// partition output into separate files based on music genre
		multipleOutputs.write(newKey, totalCount, keys[0].replaceAll("\\s",""));

	}
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
	  multipleOutputs.close();
  }
  
}