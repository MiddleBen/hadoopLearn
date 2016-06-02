package mapred;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondaryGroupCompator extends WritableComparator {

	protected SecondaryGroupCompator() {
		super(MyPairComparable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MyPairComparable a1 = (MyPairComparable) a;
		MyPairComparable a2 = (MyPairComparable) b;
		return a1.getFirst() - a2.getFirst();
	}

}
