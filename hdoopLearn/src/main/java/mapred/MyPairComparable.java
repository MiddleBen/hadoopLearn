package mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.common.IntPairWritable.Comparator;

public class MyPairComparable implements WritableComparable<MyPairComparable>,
		Cloneable {
	
	static final int INT_BYTE_LENGTH = 4;

	@Override
	public boolean equals(Object arg0) {
		// TODO Auto-generated method stub
		return super.equals(arg0);
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString();
	}

	public MyPairComparable(int first, int second) {
		super();
		this.first = first;
		this.second = second;
	}

	private int first;
	private int second;

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
	}

	@Override
	public int compareTo(MyPairComparable arg0) {
		return this.compareTo(arg0);
	}

	static {
		WritableComparator.define(IntPairWritable.class,
				new SecondaryComparator());
	}

	public static final class SecondaryComparator extends WritableComparator {

		public SecondaryComparator() {
			super(MyPairComparable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return doCompare(b1, s1, b2, s2);
		}

		static int doCompare(byte[] b1, int s1, byte[] b2, int s2) {
			int compare1 = compareInts(b1, s1, b2, s2);
			if (compare1 != 0) {
				return compare1;
			}
			return compareInts(b1, s1 + INT_BYTE_LENGTH, b2, s2
					+ INT_BYTE_LENGTH);
		}

		private static int compareInts(byte[] b1, int s1, byte[] b2, int s2) {
			// Like WritableComparator.compareBytes(), but treats first byte as
			// signed value
			int end1 = s1 + INT_BYTE_LENGTH;
			for (int i = s1, j = s2; i < end1; i++, j++) {
				int a = b1[i];
				int b = b2[j];
				if (i > s1) {
					a &= 0xff;
					b &= 0xff;
				}
				if (a != b) {
					return a - b;
				}
			}
			return 0;
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			MyPairComparable a1 = (MyPairComparable) a;
			MyPairComparable a2 = (MyPairComparable) b;
			if (a1.getFirst() != a2.getFirst()) {
				return a1.getFirst() - a2.getFirst();
			} else {
				return -(a1.getSecond() - a2.getSecond());
			}
		}
	}

}
