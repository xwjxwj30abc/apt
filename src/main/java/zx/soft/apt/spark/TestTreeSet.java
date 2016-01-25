package zx.soft.apt.spark;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

public class TestTreeSet {

	public static class PcapSub implements Comparable<PcapSub>, Comparator<PcapSub> {
		private int order;
		private byte[] value;

		public PcapSub(int order, byte[] value) {
			this.order = order;
			this.value = value;
		}

		@Override
		public int compare(PcapSub o1, PcapSub o2) {
			if (o1.order < o2.order) {
				return -1;
			} else if (o1.order > o2.order) {
				return 1;
			} else {
				return 0;
			}
		}

		@Override
		public int compareTo(PcapSub o) {
			if (order < o.order) {
				return -1;
			} else if (order > o.order) {
				return 1;

			} else {
				return 0;
			}
		}

		public int getOrder() {
			return order;
		}

		public byte[] getValue() {
			return value;
		}

		public void setOrder(int order) {
			this.order = order;
		}

		public void setValue(byte[] value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return "PcapSub [order=" + order + ", value.length=" + value.length + "]";
		}

	}

	public static void main(String[] args) {

		ByteBuffer buffer = ByteBuffer.wrap("q".getBytes());
		System.out.println(buffer.order());
		TreeSet<PcapSub> treeSet = new TreeSet<PcapSub>();
		treeSet.add(new PcapSub(39956457, "tt".getBytes()));
		treeSet.add(new PcapSub(97956545, "aa".getBytes()));
		treeSet.add(new PcapSub(-15674, "bb".getBytes()));
		treeSet.add(new PcapSub(6357455, "sa".getBytes()));
		treeSet.add(new PcapSub(5897455, "sa".getBytes()));
		Iterator<PcapSub> iterator = treeSet.iterator();
		while (iterator.hasNext()) {
			PcapSub pcapSub = iterator.next();
			System.out.println(pcapSub.order + new String(pcapSub.value));
		}
	}
}
