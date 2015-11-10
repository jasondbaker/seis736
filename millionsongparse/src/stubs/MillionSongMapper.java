package stubs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MillionSongMapper extends Mapper<LongWritable, Text, Text, Text> {

  static enum SongCounter {
	VALID, INVALID;
		
  };
	
  Text newKey = new Text();
  Text line = new Text();
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	
	// convert the tsv line to a string and then split on tabs
    String[] arr = value.toString().split("\\t");
    
      // check to make sure all the data fields are present
      if (arr.length > 52) {
        
    	  newKey.set(arr[0]);
    	  line.set(arr[3]+"\t"
    			  +arr[4]+"\t"
    			  +arr[12]+"\t"
    			  +arr[14]+"\t"
    			  +arr[28]+"\t"
    			  +arr[31]+"\t"
    			  +arr[48]+"\t"
    			  +arr[51]+"\t"
    			  +arr[53]);
    	  
    	  // record the record as valid
    	  context.getCounter(SongCounter.VALID).increment(1);
    	
    	//emit the ID as key and new TSV string as value
        context.write(newKey, line);
      } else {
    	  
    	// record doesn't contain all the fields  
    	context.getCounter(SongCounter.INVALID).increment(1);
      }
   
  }
}