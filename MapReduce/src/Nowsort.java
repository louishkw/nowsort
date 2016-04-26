/*
 * Authors: Wolf Honore, Victor Liu, Ka Wo Hong
 * Assignment: CSC 258 Project (Spring 2016)
 *
 * Description:
 *   A MapReduce implementation of the NowSort parallel external sorting algorithm.
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
    public final static int nBits = 4;

    /*
     * BucketMapper 
     *   Sorts each record into a bucket based on the first n-bits of the key.
     *   in:  record_num => record
     *   out: bucket => record
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
     *   in:  bucket => records
     *   out: null => record
     */
    public static class SortReducer 
            extends Reducer<LongWritable, BytesWritable, NullWritable, Text> {
        Text outRecord = new Text();

        public void reduce(LongWritable bucket, Iterable<BytesWritable> records, Context context)
                throws IOException, InterruptedException {
            ArrayList<byte[]> sortedRecords = new ArrayList<>();

            /* Copy records */
            for (BytesWritable record : records) {
                sortedRecords.add(record.copyBytes());
            }

            /* Sort records */
            sort(sortedRecords);

            /* Output sorted records */
            for (byte[] record : sortedRecords) {
                /* Convert binary to text */
                outRecord.set(record);
                context.write(NullWritable.get(), outRecord);
            }
        } 

        /*
         * compareRec()
         *   Compares the keys of two records byte-by-byte and returns -1, 1, or 0 if rec1 is 
         *   correspondingly less than, greater than, or equal to rec2.
         */
        private int compareRec(byte[] rec1, byte[] rec2) {
            for (int i = 0; i < KEY_SIZE; i++) {
                /* Have to cast to int because bytes are signed in Java */
                int k1 = (int) (rec1[i] & 0xFF);
                int k2 = (int) (rec2[i] & 0xFF);

                if (k1 < k2) {
                    return -1;
                }
                else if (k1 > k2) {
                    return 1;
                }
            }

            return 0;
        }
        
        /*
         * sort()
         *   Sorts a list of records by their keys using insertion sort.
         */
        private void sort(ArrayList<byte[]> records) {
            for (int i = 0; i < records.size(); i++) {
                for (int j = i; j > 0; j--) {
                    if (compareRec(records.get(j - 1), records.get(j)) > 0) {
                        byte[] tmp = records.get(j - 1);
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
        Path outPath = new Path("out");
        int nRecs = new Integer(args[1]); // May not need this
        int nBuckets = (1 << nBits);
        int nBucketPerReducer = 1;

        Configuration conf = new Configuration();
        conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, REC_SIZE);

        Job job = Job.getInstance(conf, "nowsort");
        job.setJarByClass(Nowsort.class);
        job.setMapperClass(BucketMapper.class);
        //job.setCombinerClass(SortReducer.class);
        job.setReducerClass(SortReducer.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(FixedLengthInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class); // TODO: may need to make a custom
                                                          // output format to avoid newlines

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        //FileInputFormat.setMaxInputSplitSize(job, /* splitsize */);
        // TODO: may need to make another pass to combine files, or just use -getmerge
        job.setNumReduceTasks(nBuckets / nBucketPerReducer);

        job.waitForCompletion(true);
    }
}
