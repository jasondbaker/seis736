package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MillionSongTermCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  static enum SongCounter {
	VALID, INVALID;
		
  };
	
  Text newKey = new Text();
  Text line = new Text();
  String title;
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	
	Text term = new Text();
	IntWritable one = new IntWritable(1);
	
	// convert the tsv line to a string and then split on tabs
    String[] arr = value.toString().split("\\t");
      
      // confirm that all the data fields are present
      if (arr.length > 9) {
        
    	  // get the track artist terms
    	  String[] terms = arr[4].split(",");
    	  
    	  // retrieve the first term from the list
    	  // this is the most strongly associated term
    	  if (terms[0].length() > 0) {
    		  term.set(terms[0].toLowerCase().trim());
    		  
    		  //emit the term as the key with a value of 1
    		  context.write(term, one);
    		  
    		  //record a valid record
    		  context.getCounter(SongCounter.VALID).increment(1);
    	  } else {
    		  
    		  //the record was missing terms
    		  context.getCounter(SongCounter.INVALID).increment(1);
    	  }
    	  
      } else {
    	  
    	// record doesn't contain all the fields  
    	context.getCounter(SongCounter.INVALID).increment(1);
      }
   
  }
}