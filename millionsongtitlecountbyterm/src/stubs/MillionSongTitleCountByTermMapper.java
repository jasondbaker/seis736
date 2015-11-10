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

public class MillionSongTitleCountByTermMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

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
			 
			  context.getCounter(SongCounter.FILE_NOT_FOUND).increment(1);
		  }			  
	  }
	    
  }
  
  private void loadStopwordsHashSet(Path filePath, Context context) throws IOException {
	
	  String strLineRead = "";
	  
	  try {
		  //create a bufferedreader to read the text file
		  brReader = new BufferedReader(new FileReader(filePath.toString()));
		  
		  while ((strLineRead = brReader.readLine()) != null) {
			  String stopword = strLineRead.trim();
			  
			  //add each word to the hashset
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
    	  
    	  // retrieve each word from the title
    	  for (String word : title.split("\\W+")) {
    	      if (word.length() > 0) {
    	    	
    	    	// check to see if word is a stopword
    	    	word = word.toLowerCase();
    	    	if (!(stopwords.contains(word))) {
    	    		
    	      	  // get the track artist terms
    	      	  String[] terms = arr[4].split(",");
    	      	  
    	      	  // retrieve the first term from the list
    	      	  // this is the most strongly associated term
    	      	  if (terms[0].length() > 0) {
    	      		  
			    	  // emit term and single word in CSV
			    	  titleword.set(terms[0].toLowerCase().trim()+","+word);
			    	  context.write(titleword, one);
    	      	  } else {
    	      		// the record doesn't have any terms
    	      		context.getCounter(SongCounter.INVALID).increment(1);
    	      	  }
    	    	}
    	    	else {
    	    		// the word was filtered by the stop word list
    	    		context.getCounter(SongCounter.STOPWORD).increment(1);
    	    	}
    	      }
    	    }
    	  // the record was valid
    	  context.getCounter(SongCounter.VALID).increment(1);

      } else {
    	  
    	// record doesn't contain all the fields  
    	context.getCounter(SongCounter.INVALID).increment(1);
      }
   
  }
}