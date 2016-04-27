/*
 * Authors: Wolf Honore, Victor Liu, Ka Wo Hong
 * Assignment: CSC 258 Project (Spring 2016)
 *
 * Description:
 *   A MapReduce implementation of the NowSort parallel external sorting algorithm.
 */

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Nowsort {
    public final static int KEY_SIZE = 10;
    public final static int REC_SIZE = 100;

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
            int nBits = context.getConfiguration().getInt("nBits", 0);

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
        NullWritable outKey = NullWritable.get();
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
                context.write(outKey, outRecord);
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

    /*
     * RecordOutputFormat
     *   Custom output format that writes records with no key and no newline.
     */
    private static class RecordOutputFormat 
            extends FileOutputFormat<NullWritable, Text> {
        public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext job)
                throws IOException {
            Path file = this.getDefaultWorkFile(job, ".dat");
            
            FileSystem fs = file.getFileSystem(job.getConfiguration());
            FSDataOutputStream fileOut = fs.create(file, job);

            return new RecordRecordWriter(fileOut);
        }
    }

    /*
     * RecordRecordWriter
     *   Custom record writer that writes records with no key and no newline.
     */
    private static class RecordRecordWriter
            extends RecordWriter<NullWritable, Text> {
        private DataOutputStream out;

        public RecordRecordWriter(DataOutputStream stream) {
            this.out = stream;
        }

        public void close(TaskAttemptContext context) throws IOException {
            out.close();
        }

        public void write(NullWritable key, Text record) throws IOException {
            out.write(record.getBytes(), 0, record.getLength());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("java Nowsort <file> <outdir> <nBits>");
            System.exit(1);
        }
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        int nBits = new Integer(args[2]); 
        int nBuckets = (1 << nBits);

        Configuration conf = new Configuration();
        conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, REC_SIZE);
        conf.setInt("nBits", nBits);

        Job job = Job.getInstance(conf, "nowsort");
        job.setJarByClass(Nowsort.class);
        job.setMapperClass(BucketMapper.class);
        job.setReducerClass(SortReducer.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(FixedLengthInputFormat.class);
        job.setOutputFormatClass(RecordOutputFormat.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        //FileInputFormat.setMaxInputSplitSize(job, /* splitsize */);
        // TODO: may need to make another pass to combine files, or just use -getmerge
        job.setNumReduceTasks(nBuckets);

        job.waitForCompletion(true);
    }
}
