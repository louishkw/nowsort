/*
 * Authors: Wolf Honore, Victor Liu, Ka Wo Hong
 * Assignment: CSC 258 Project (Spring 2016)
 *
 * Description:
 *  
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Nowsort {
    public final static int KEY_SIZE = 10;
    public final static int REC_SIZE = 100;

    /*
     * BucketMapper 
     */
    public static class BucketMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) 
                throws IOException, InterruptedException {
        }
    }

    /*
     * SortReducer
     */
    public static class SortReducer 
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        }   
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("java Nowsort <file> <nrecords>");
            System.exit(1);
        }
        Path inPath = new Path(args[0]);
        Path outPath = new Path("out.dat");
        int nrecs = new Integer(args[1]);

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "nowsort");
        job.setJarByClass(Nowsort.class);
        job.setMapperClass(BucketMapper.class);
        job.setReducerClass(SortReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(FixedLengthInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FixedLengthInputFormat.setRecordLength(conf, REC_SIZE);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        //FileInputFormat.setMaxInputSplitSize(job, /* splitsize */);
        //job.setNumReduceTasks(/* numreduce */);

        job.waitForCompletion(true);
    }
}
