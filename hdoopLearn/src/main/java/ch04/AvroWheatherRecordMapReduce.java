package ch04;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroUtf8InputFormat;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.util.ajax.JSON;

import utils.FileUtils;

import ch04.entity.WeatherRecord;

public class AvroWheatherRecordMapReduce extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AvroWheatherRecordMapReduce(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		FileUtils.delAllFile(args[1]);
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("Max temperature");
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		AvroJob.setInputSchema(conf, Schema.create(Schema.Type.STRING));
		AvroJob.setMapOutputSchema(conf, Pair.getPairSchema( Schema.create(Schema.Type.INT), WeatherRecord.SCHEMA$));
		AvroJob.setOutputSchema(conf, WeatherRecord.SCHEMA$);
		conf.setInputFormat(AvroUtf8InputFormat.class);
		AvroJob.setMapperClass(conf, WheatherRecordMapper.class);
		AvroJob.setReducerClass(conf, WheatherRecordReducer.class);
		JobClient.runJob(conf);
		return 0;
	}

	static class WheatherRecordMapper extends
			AvroMapper<Utf8, Pair<Integer, WeatherRecord>> {

		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		public void map(Utf8 datum,
				AvroCollector<Pair<Integer, WeatherRecord>> collector,
				Reporter reporter) throws IOException {
			parser.parse(datum.toString());
			if (parser.isValidTemperature()) {
				System.out.println("---------------------" + datum.toString()
						+ "----------------------");
				WeatherRecord weatherRecord = new WeatherRecord();
				weatherRecord.setYear(parser.getYearInt());
				weatherRecord.setStationId(parser.getStationId());
				weatherRecord.setTemperature(parser.getAirTemperature());
				collector.collect(new Pair<Integer, WeatherRecord>(
						parser.getYearInt(), weatherRecord));
			}
		}

	}

	static class WheatherRecordReducer extends
			AvroReducer<Integer, WeatherRecord, WeatherRecord> {

		@Override
		public void reduce(Integer key, Iterable<WeatherRecord> values,
				AvroCollector<WeatherRecord> collector, Reporter reporter)
				throws IOException {
			WeatherRecord max = null;
			System.out.println("++++++++++++++++++++++" + JSON.toString(values)
					+ "++++++++++++++++++++++++");
			for (WeatherRecord value : values) {
				if (max == null
						|| (Integer) value.getTemperature() > (Integer) max
								.getTemperature()) {
					max = value;
				}
			}
			collector.collect(max);
		}

	}

}
