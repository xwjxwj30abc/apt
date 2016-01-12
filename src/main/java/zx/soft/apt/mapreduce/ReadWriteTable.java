package zx.soft.apt.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * mapreduce读写hbase表
 * @author fgq
 *
 */
public class ReadWriteTable {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = HBaseConfiguration.create();
		Job job = Job.getInstance(config, "ExampleReadWriteTable");
		job.setJarByClass(ReadWriteTable.class);
		Scan scan = new Scan();
		scan.setStartRow("000000000000001020200000000013226313020000385423410198165".getBytes());
		scan.setStopRow("00000000000000102020000000001322631302000985423410198165".getBytes());
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob("sina_weibo_100million", scan, MyMapper.class, Text.class,
				IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("mapreduce_count_weibo", MyTableReducer.class, job);
		job.setNumReduceTasks(1);
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job");
		}
	}

	public static class MyMapper extends TableMapper<Text, IntWritable> {

		private final IntWritable ONE = new IntWritable(1);
		private Text text = new Text();

		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,
				InterruptedException {

			String val = new String(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("so")));
			text.set(val);
			context.write(text, ONE);
			/*		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> temps = value.getMap();

					String rowkey = Bytes.toString(value.getRow());

					System.out.println("rowkey->" + rowkey);

					for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> temp : temps.entrySet()) {

						System.out.print("\tfamily->" + Bytes.toString(temp.getKey()));

						for (Entry<byte[], NavigableMap<Long, byte[]>> valu : temp.getValue().entrySet()) {

							System.out.print("\tcol->" + Bytes.toString(valu.getKey()));

							for (Entry<Long, byte[]> va : valu.getValue().entrySet()) {
								System.out.print("\tvalue->" + Bytes.toString(va.getValue()));
								System.out.println();
							}
						}
					}*/

		}
	}

	public static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int i = 0;
			for (IntWritable val : values) {
				i += val.get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(i));
			context.write(null, put);
		}
	}
}
