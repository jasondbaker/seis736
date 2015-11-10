package stubs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MillionSongTitleCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static HashSet<String> stopwords = new HashSet<String>();
  private BufferedReader brReader;
  static enum SongCounter {
	VALID, INVALID, STOPWORD, STOPWORDS_EXIST, FILE_NOT_FOUND, IO_ERROR;
		
  };
	
  Text newKey = new Text();
  Text line = new Text();
  String title;
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
	  
	  Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	  
	  // find the stopwords text file in the cache
	  for (Path eachPath : cacheFilesLocal) {
		  if (eachPath.toUri().getPath().endsWith("stopwords.txt")) {
			  context.getCounter(SongCounter.STOPWORDS_EXIST).increment(1);
			  loadStopwordsHashSet(eachPath, context);
		  } else {
			 
			  // the stopwords file is missing in the cache
			  context.getCounter(SongCounter.FILE_NOT_FOUND).increment(1);
		  }
			  
	  }
	  
	  
  }
  
  private void loadStopwordsHashSet(Path filePath, Context context) throws IOException {
	
	  String strLineRead = "";
	  
	  try {
		  // create a new bufferedreader to read from the file
		  brReader = new BufferedReader(new FileReader(filePath.toString()));
		  
		  while ((strLineRead = brReader.readLine()) != null) {
			  String stopword = strLineRead.trim();
			  
			  // add each word in the file to the hashset
			  stopwords.add(stopword);
		  }
	  } catch (FileNotFoundException e) {
		  e.printStackTrace();
		  context.getCounter(SongCounter.FILE_NOT_FOUND).increment(1);
	  } catch (IOException e) {
		  e.printStackTrace();
		  context.getCounter(SongCounter.IO_ERROR).increment(1);
	  } finally {
		  if (brReader != null) {
			  brReader.close();
		  }
	  }
  }
  
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	
	Text titleword = new Text();
	IntWritable one = new IntWritable(1);
	
	// convert the tsv line to a string and then split on tabs
    String[] arr = value.toString().split("\\t");
    
      // verify that all the data fields are present
      if (arr.length > 9) {
        
    	  // get the song title
    	  title = arr[8];
    	  
    	  // retrieve each word from the song title
    	  for (String word : title.split("\\W+")) {
    	      if (word.length() > 0) {
    	    	
    	    	// check to see if word is a stopword
    	    	word = word.toLowerCase();
    	    	
    	    	if (!(stopwords.contains(word))) {
    	    		// emit single word as key and value of 1
    	    		titleword.set(word);
    	    		context.write(titleword, one);
    	    	}
    	    	else {
    	    		// record that the word was filtered
    	    		context.getCounter(SongCounter.STOPWORD).increment(1);
    	    	}
    	      }
    	    }
    	  // record that the record was valid
    	  context.getCounter(SongCounter.VALID).increment(1);

      } else {
    	  
    	// record doesn't contain all the fields  
    	context.getCounter(SongCounter.INVALID).increment(1);
      }
   
  }
}