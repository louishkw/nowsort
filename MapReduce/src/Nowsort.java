/*
 * Authors: Wolf Honore, Victor Liu, Ka Wo Hong
 * Assignment: CSC 258 Project (Spring 2016)
 *
 * Description:
 * 
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
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
    public final static int nBits = 16;

    /*
     * BucketMapper 
     *   Sorts each record into a bucket based on the first n-bits of the key.
     */
    public static class BucketMapper 
            extends Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable> {
        private LongWritable bucket = new LongWritable();

        public void map(LongWritable key, BytesWritable record, Context context) 
                throws IOException, InterruptedException {
            /* Extract the key from the record */
            ByteBuffer data = ByteBuffer.wrap(record.getBytes(), 0, KEY_SIZE);

            /* Choose a bucket based on the top n-bits */
            bucket.set(data.getLong() >> (64 - nBits)); 

            /* bucket => record */
            context.write(bucket, record);
        }
    }

    /*
     * SortReducer
     *   Sorts each bucket by comparing the 10-byte keys.
     */
    public static class SortReducer 
            extends Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {
        public void reduce(LongWritable bucket, Iterable<BytesWritable> records, Context context)
                throws IOException, InterruptedException {
            ArrayList<BytesWritable> sortedRecords = new ArrayList<>();

            /* Copy records */
            for (BytesWritable record : records) {
                sortedRecords.add(record);
            }

            /* Sort records */
            sort(sortedRecords);

            /* Output sorted records */
            for (BytesWritable record : sortedRecords) {
                context.write(bucket, record);
            }
        } 

        private void sort(ArrayList<BytesWritable> records) {
            for (int i = 0; i < records.size(); i++) {
                for (int j = i; j > 0; j--) {
                    ByteBuffer key1 = ByteBuffer.wrap(records.get(j - 1).getBytes(), 0, KEY_SIZE);
                    ByteBuffer key2 = ByteBuffer.wrap(records.get(j).getBytes(), 0, KEY_SIZE);

                    if (key1.compareTo(key2) > 0) {
                        BytesWritable tmp = records.get(j - 1);
                        records.set(j - 1, records.get(j));
                        records.set(j, tmp);
                    }
                    else {
                        break;
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("java Nowsort <file> <nrecords>");
            System.exit(1);
        }
        Path inPath = new Path(args[0]);
        Path outPath = new Path("out.dat");
        int nRecs = new Integer(args[1]); // May not need this
        int nBuckets = (1 << nBits);
        int nBucketPerReducer = 1;

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "nowsort");
        job.setJarByClass(Nowsort.class);
        job.setMapperClass(BucketMapper.class);
        //job.setCombinerClass(SortReducer.class);
        job.setReducerClass(SortReducer.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(FixedLengthInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class); // Probably need to change this
        
        FixedLengthInputFormat.setRecordLength(conf, REC_SIZE);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        //FileInputFormat.setMaxInputSplitSize(job, /* splitsize */);
        job.setNumReduceTasks(nBuckets / nBucketPerReducer);

        job.waitForCompletion(true);
    }
}
