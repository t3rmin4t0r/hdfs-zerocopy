package org.notmysock.hdfs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.reduce.*;

import org.apache.commons.cli.*;
import org.apache.commons.*;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.net.*;
import java.math.*;
import java.security.*;


public class ZCRTest extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new ZCRTest(), args);
        System.exit(res);
    }

    static final class OneRecordReader 
      extends RecordReader<LongWritable, LongWritable> {
      private InputSplit split = null;
      private boolean first = false;
      public void initialize(InputSplit split, TaskAttemptContext context) {
        this.split = split;
      }
      public void close() {
      }
      public LongWritable getCurrentKey() {
        return new LongWritable(0);
      }
      public LongWritable getCurrentValue() {
        return new LongWritable(0);
      }
      public boolean nextKeyValue() {
        if(first == false) {
          first = true;
          return true;
        } else {
          return false;
        }
      }
      public float getProgress() {
        if(first == false) return 0.0f;
        return 1.0f;
      }
    }

    static final class UnsplittableInputFormat
      extends FileInputFormat<LongWritable,LongWritable> {
      @Override
      protected boolean isSplitable(JobContext job, Path filename) {
        return false;
      }

      @Override
      public RecordReader<LongWritable,LongWritable> 
        createRecordReader(InputSplit split, TaskAttemptContext context) {
        RecordReader<LongWritable, LongWritable> rr = new OneRecordReader();
        return rr;
      }
    }

    static final class ByteByByteReader extends Mapper<LongWritable,LongWritable, Text, Text> {
      protected void map(LongWritable offset, LongWritable value, Mapper.Context context) 
        throws IOException, InterruptedException {
        InputSplit split = context.getInputSplit();
        Configuration conf =  context.getConfiguration(); 
        long bs = conf.getLong("org.notmysock.hdfs.readsize", 1);
        byte[] buf = new byte[1];
        if(bs > 1) {
          buf = new byte[(int)bs];
        }
        Path p = ((FileSplit)split).getPath();
        FileSystem fs = p.getFileSystem(conf);
        FSDataInputStream in = fs.open(p);
        long t1 = System.currentTimeMillis();
        if(buf.length == 1) {
          while(in.available() > 0) in.readByte();          
        } else {
          while(in.read(buf) !=  -1);
        }
        long size = in.getPos();
        long t2 = System.currentTimeMillis();
        context.write("Time Taken: ", (t2 - t1)+" ms");
        context.write("Data read: ", size +" bytes");
      }
    }

    static final class ZeroCopyReader extends Mapper<LongWritable,LongWritable, Text, Text> {
      protected void map(LongWritable offset, LongWritable value, Mapper.Context context) 
        throws IOException, InterruptedException {
        InputSplit split = context.getInputSplit();
        Configuration conf =  context.getConfiguration(); 
        Path p = ((FileSplit)split).getPath();
        FileSystem fs = p.getFileSystem(conf);
        FSDataInputStream in = fs.open(p);
        long bs = conf.getLong("org.notmysock.hdfs.readsize", 1);
        long t1 = System.currentTimeMillis();
        ByteBufferPool bfpool = new ElasticByteBufferPool();
        EnumSet<ReadOption> rdopts = EnumSet.noneOf(ReadOption.class);
        if(conf.getBoolean("dfs.client.read.shortcircuit.skip.checksum", false)) {
            rdopts = EnumSet.of(ReadOption.SKIP_CHECKSUMS);
        }
        while(in.available() > 0) {
          ByteBuffer bbuf = in.read(bfpool, (int)bs, rdopts);           
          in.releaseBuffer(bbuf);
        }
        in.close();
        long size = in.getPos();
        long t2 = System.currentTimeMillis();
        context.write("Time Taken: ", (t2 - t1)+" ms");
        context.write("Data read: ", size +" bytes");
      }
    }

    @Override
    public int run(String[] args) throws Exception {
        String[] remainingArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

        CommandLineParser parser = new BasicParser();
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
        options.addOption("i","input", true, "input");
        options.addOption("bs", "blocksize", true, "blocksize");
        options.addOption("o", "output", true, "output");
        options.addOption("nc", "nochecksum", false, "nochecksum");
        options.addOption("zc", "zerocopy", false, "zerocopy");
        CommandLine line = parser.parse(options, remainingArgs);

        if(!line.hasOption("input")) {
          HelpFormatter f = new HelpFormatter();
          f.printHelp("ZCRTest", options);
          return 1;
        }

        Path in = new Path(line.getOptionValue("input"));
        Path out;        
        int bs;
	      boolean nc;

        nc = line.hasOption("nochecksum");
        
        if(line.hasOption("blocksize")) {
          bs = Integer.parseInt(line.getOptionValue("blocksize"));
        } else {
          bs = 1;
        }
        
        if(line.hasOption("output")) {
          out = new Path(line.getOptionValue("output"));
        } else {
          String random = Long.toHexString(Double.doubleToLongBits(Math.random()));
          out = new Path("/tmp/shortcircuit-"+random);
        }

        Configuration conf = getConf();
        
        conf.setLong("org.notmysock.hdfs.readsize", bs);
        conf.setBoolean("dfs.client.read.shortcircuit.skip.checksum", nc);

        Job job = new Job(conf, "ZCRTest");
        job.setJarByClass(getClass());
        job.setNumReduceTasks(0);
        if(line.hasOption("zerocopy")) {
          job.setMapperClass(ZeroCopyReader.class);
        } else {
          job.setMapperClass(ByteByByteReader.class);
        }
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(UnsplittableInputFormat.class);

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);

        boolean success = job.waitForCompletion(true);

        FileSystem fs = FileSystem.get(getConf());
        FSDataInputStream result = fs.open(new Path(out, "part-m-00000"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(result));
        String l;
        while((l = reader.readLine()) != null) {
          System.out.println(l);
        }
        return 0;
    }
}
