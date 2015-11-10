package stubs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;


public class TestMillionSongTermCount {

  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
 
  /*
   * Set up the test. This method will be called before every test.
   */
  @Before
  public void setUp() {

    // Set up the mapper test harness.
    MillionSongTermCountMapper mapper = new MillionSongTermCountMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
    mapDriver.setMapper(mapper);

    
    // Set up the reducer test harness.
    MillionSongTermCountReducer reducer = new MillionSongTermCountReducer();
    reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
    reduceDriver.setReducer(reducer);
	
  }

  /*
   * Test the mapper.
   */
  @Test
  public void testMapper() {

	// use some real test data
	Text line = new Text();
	line.set("TRAAAAY128F42A73F0"+"\t"
	+"0.476940591"+"\t"
	+"0.307080169"+"\t"
	+"Alquimia"+"\t"
	+"chill-out,cumbia,downtempo,trip hop,salsa,latin jazz,power metal,symphonic metal,electronic,easy listening,electro,ambient,trance,post rock,experimental,psychedelic rock,heavy metal,jazz,world,folk,country,rock,alternative rock,indie rock,female vocalist,modern classical,new age,ethereal,tribal,melancholia,psychedelic trance,metal,house,latin,noise,mexico,tropical,chile,gothic rock,colombia,musica colombiana,esoteric,mexican divas,salsa colombiana,latin electronic,world reggae"+"\t"
	+"-13.179"+"\t"
	+"Forever"+"\t"
	+"41.279"+"\t"
	+"The Lark In The Clear Air"+"\t"
	+"0");
	
	
    mapDriver.withInput(new LongWritable(), line);
    mapDriver.withOutput(new Text("chill-out"), new IntWritable(1) );

    mapDriver.runTest();

  }
  
  /*
   * Test the reducer.
  */ 
  @Test
  public void testReducer() {

    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    reduceDriver.withInput(new Text("chill-out"), values);
    reduceDriver.withOutput(new Text("chill-out"), new IntWritable(2));
    reduceDriver.runTest();

  }
  
}
