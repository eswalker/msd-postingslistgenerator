package com.eswalker.msd.PostingsListGenerator;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PostingsListGenerator extends Configured implements Tool{
	public static class HMapper extends Mapper<LongWritable, Text, Text, Text> {	     
		
		private static Text outputKey = new Text();
	    private static Text outputValue = new Text();
	   
	   
	    @Override
	    protected final void setup(final Context context) throws IOException, InterruptedException {
	  
	    }

		@Override
		/**
		 * PostingsListGenerator mapper
		 * 
		 * Function = key by tag
		 * 
		 * In Value = track_id|lastfm_id|artist|title|num_tags|tag1,score1|tag2,score2|...
		 * 
		 * Out Key = tag
		 * Out Value = track_id,score
		 */
		public final void map(final LongWritable key, final Text value, Context context) throws IOException, InterruptedException {
			
			String data[] = Split.charSplit(value.toString(), '|');
			String trackID = data[0];
			for (int i = 5; i < data.length; i++) {
				String tagAndScore = data[i];
				String data2[] = Split.commaSplit(tagAndScore);
				if (data2.length == 2) {
					String tag = data2[0];
					String score = data2[1];
					outputKey.set(tag);
					outputValue.set(trackID + "," + score);
					context.write(outputKey, outputValue);
				}
			}
			
		}
		
	}

	
	public static class HReducer extends Reducer<Text, Text, NullWritable, Text> {
		private static NullWritable nullKey = NullWritable.get();
        private static Text outputValue = new Text();
        
        @Override
       /***
        * PostingsListGenerator reducer
        * 
        * Function = concatenate values
        * 
        * In key = tag
        * In values = track_id,score
        * 
        * Out value = tag|track_id1,score1|track_id2,score2|...
        *
        */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
        	
        	StringBuilder sb = new StringBuilder();
        	sb.append(key.toString()); sb.append('|');
        	for (Text t : values) {
        		sb.append(t.toString()); sb.append('|');
        	}
        	sb.deleteCharAt(sb.length()-1);
        	
        	outputValue.set(sb.toString());
        	context.write(nullKey, outputValue);
        }
        
    }

    /**
     * Sets up job, input and output.
     * 
     * @param args
     *            inputPath outputPath
     * @throws Exception
     */
	public int run(String[] args) throws Exception {

       Configuration conf = getConf();

       if (args != null) {
           for (String a : args) { System.out.println("arg: " + a); }
       } else {
           System.out.println("args is null");
       }
        
       Options options = new Options();
      

       CommandLineParser parser = new PosixParser();
       CommandLine cmd = parser.parse(options, args);
   

    	
        
       Job job = new Job(conf);
       job.setJarByClass(PostingsListGenerator.class);
       job.setJobName("PostingsListGenerator");


       
       job.setInputFormatClass(TextInputFormat.class);


       TextInputFormat.addInputPaths(job, args[0]);
       TextOutputFormat.setOutputPath(job, new Path(args[1]));


       job.setMapperClass(HMapper.class);
       job.setReducerClass(HReducer.class);

       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(Text.class);
       job.setOutputKeyClass(NullWritable.class);
       job.setOutputValueClass(Text.class);

       return job.waitForCompletion(true) ? 0 : 1;
    }
	
    public static void main(String args[]) throws Exception {
    	
        if (args.length < 2) {
            System.out.println("Usage: PostingsListGenerator <input dirs> <output dir>");
            System.exit(-1);
        }
 
        int result = ToolRunner.run(new PostingsListGenerator(), args);
        System.exit(result);
        
   }


}

