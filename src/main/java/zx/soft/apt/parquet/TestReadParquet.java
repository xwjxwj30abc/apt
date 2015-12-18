package zx.soft.apt.parquet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleInputFormat;

public class TestReadParquet {

	public static class MyMap extends Mapper<LongWritable, Group, NullWritable, Text> {

		@Override
		public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
			NullWritable outKey = NullWritable.get();
			String inputRecord = value.toString();
			System.out.println(inputRecord);
			context.write(outKey, new Text(inputRecord));
		}
	}

	//读取parquet文件并存储为text文件
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException,
			InterruptedException {

		String inputfile = "hdfs://archtorm:9000/user/xuwenjuan/mapreduce/output_parquet/part-m-00000.snappy.parquet";
		String outputfile = "hdfs://archtorm:9000/user/xuwenjuan/mapreduce/outputTextByParquet";

		Configuration conf = new Configuration();
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		Job job = new Job(conf);
		job.setJarByClass(TestReadParquet.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MyMap.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(ExampleInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputfile));
		FileOutputFormat.setOutputPath(job, new Path(outputfile));

		job.waitForCompletion(true);
	}
}