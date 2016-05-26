package ch04;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.FileUtils;
import ch04.entity.WeatherRecord;

/**
 * mapper 和reducer为啥一定要是static类才能执行，否则例如为public类就报错。
 * @author Administrator
 *
 */
public class AvroDataSortMapReduce extends Configured implements Tool {

	public static void main(String[] args) throws Throwable {
		int exitCode = ToolRunner.run(new AvroDataSortMapReduce(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("usage: ./avroSortMapReduce inPath outPaht schemaPath");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		FileUtils.delAllFile(args[1]);
		String input = args[0];
		String output = args[1];
		String schemaFile = args[2];
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("avro map reduce sort");
		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		Schema schema = new Schema.Parser().parse(new File(schemaFile));
		AvroJob.setInputSchema(conf, schema);
		Schema pairSchema = Pair.getPairSchema(schema, schema);
		
		AvroJob.setMapOutputSchema(conf, pairSchema);
		AvroJob.setOutputSchema(conf, schema);

		AvroJob.setMapperClass(conf, SortMapper.class);
		AvroJob.setReducerClass(conf, SortReducer.class);

		JobClient.runJob(conf);
		return 0;
	}

	public class SortMapper extends
			AvroMapper<WeatherRecord, Pair<WeatherRecord, WeatherRecord>> {

		@Override
		public void map(WeatherRecord datum,
				AvroCollector<Pair<WeatherRecord, WeatherRecord>> collector,
				Reporter reporter) throws IOException {
			collector.collect(new Pair<WeatherRecord, WeatherRecord>(datum, datum));
		}

	}

	public class SortReducer extends
			AvroReducer<WeatherRecord, WeatherRecord, WeatherRecord> {

		@Override
		public void reduce(WeatherRecord key, Iterable<WeatherRecord> values,
				AvroCollector<WeatherRecord> collector, Reporter reporter)
				throws IOException {
			for (WeatherRecord value : values) {
				collector.collect(value);
			}
		}

	}

}
