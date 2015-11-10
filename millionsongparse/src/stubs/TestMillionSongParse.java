package stubs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import org.junit.Before;
import org.junit.Test;


public class TestMillionSongParse {

  /*
   * Declare harnesses that let you test a mapper, a reducer, and
   * a mapper and a reducer working together.
   */
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
 
  /*
   * Set up the test. This method will be called before every test.
   */
  @Before
  public void setUp() {

    /*
     * Set up the mapper test harness.
     */
    MillionSongMapper mapper = new MillionSongMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
    mapDriver.setMapper(mapper);
  }

  /*
   * Test the mapper.
   */
  @Test
  public void testMapper() {

	
	Text line = new Text();
	String values = "";
	
	// create a tab-delimited string of integers
	for (int i = 0; i< 54; i++){
		values = values + Integer.toString(i) + "\t";
	}
	
	line.set(values);
	
    mapDriver.withInput(new LongWritable(), line);
    mapDriver.withOutput(new Text("0"), new Text("3	4	12	14	28	31	48	51	53"));

    mapDriver.runTest();

  }
  
}
