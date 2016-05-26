package ch04;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.classifier.df.data.DataUtils;

import ch04.entity.WeatherRecord;

public class AvroDataSortMapReduce extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		return 0;
	}
	
	public class SortMapper extends AvroMapper<WeatherRecord, Pair<Integer, Text>> {

		@Override
		public void map(WeatherRecord datum, AvroCollector<Pair<Integer, Text>> collector,
				Reporter reporter) throws IOException {
			collector.collect(new Pair<Integer, Text>(Integer.valueOf(datum.getYear()), new Text(datum.getTemperature().toString())));
		}
		
	}
	
	public class SortReducer extends AvroReducer<Integer, Text, Pair<Integer, Text>> {

		@Override
		public void reduce(Integer key, Iterable<Text> values,
				AvroCollector<Pair<Integer, Text>> collector, Reporter reporter)
				throws IOException {
//			collector.collect(key, values)
		}
		
	}

}
