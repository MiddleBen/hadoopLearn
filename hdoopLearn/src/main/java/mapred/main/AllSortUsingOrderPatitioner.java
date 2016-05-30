package mapred.main;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AllSortUsingOrderPatitioner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.print("usage : ./MultipleOutput input output");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJobName("AllSortUsingOrderPatitioner");
		job.setJarByClass(MultipleOutputMapReduce.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000,
				10);
		InputSampler.writePartitionFile(job, sampler);
		// Add to DistributedCache
		String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
		URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
		DistributedCache.addCacheFile(partitionUri, conf);
		DistributedCache.createSymlink(conf);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AllSortUsingOrderPatitioner(), args);
		System.exit(exitCode);
	}

}
