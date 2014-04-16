/**
 * 
 */
package com.eswalker.msd.PostingsListGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.eswalker.msd.PostingsListGenerator.PostingsListGenerator.HMapper;
import com.eswalker.msd.PostingsListGenerator.PostingsListGenerator.HReducer;


 
public class PostingsListGeneratorTest {
 
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, Text, NullWritable, Text> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapReduceDriver;
 
  @Before
  public void setUp() throws ParseException {

  
    HMapper mapper = new HMapper();
    HReducer reducer = new HReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  
  }
 
  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(
        "1|TRAID|artist|title|1|hello,100"
    ));

    mapDriver.withOutput(new Text("hello"), new Text("1,100,1,100"));
    mapDriver.runTest();
  }
  
 
	  
  
  @Test
  public void testReducer() throws IOException {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("1,100,1,10"));
    values.add(new Text("2,100,1,100"));
    values.add(new Text("3,75,1,100"));
    reduceDriver.withInput(new Text("hello"), values );
    reduceDriver.withOutput(NullWritable.get(), new Text("hello|2,100|1,100|3,75"));
    reduceDriver.runTest();
  }
  
 
}