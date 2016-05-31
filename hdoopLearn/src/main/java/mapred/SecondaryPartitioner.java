package mapred;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondaryPartitioner extends Partitioner<IntPairComparable, NullWritable> {

	@Override
	public int getPartition(IntPairComparable key, NullWritable value,
			int numPartitions) {
		return key.getFirst() % numPartitions;
	}
	

}
