package mapred.main;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.*;

import utils.FileUtils;

/**
 * 通过采样器做全局分区排序，某个分区必然不小于另外一个分区（不包括最大最小），以到达全局有序
 * 
 * @author Administrator
 * 
 */
public class SortByTemperatureUsingTotalOrderPartitioner extends Configured
		implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.print("usage : ./SortByTemperatureUsingTotalOrderPartitioner input output");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJobName("AllSortUsingOrderPatitioner");
		job.setJarByClass(MultipleOutputMapReduce.class);
		FileUtils.delAllFile(args[1]);
		String input = args[0];
		String output = args[1];
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
//		job.setNumReduceTasks(20);
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(
				0.1, 10000, 10);
		InputSampler.writePartitionFile(job, sampler);
		String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
		URI partitionUri = new URI(partitionFile + "#"
				+ TotalOrderPartitioner.DEFAULT_PATH);
		DistributedCache.addCacheFile(partitionUri, conf);
		DistributedCache.createSymlink(conf);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(
				new SortByTemperatureUsingTotalOrderPartitioner(), args);
		System.exit(exitCode);
	}
}
// ^^ SortByTemperatureUsingTotalOrderPartitioner
