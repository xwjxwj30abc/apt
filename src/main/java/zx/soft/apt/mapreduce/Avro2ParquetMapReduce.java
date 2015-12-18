package zx.soft.apt.mapreduce;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class Avro2ParquetMapReduce {

	//mapreduce读取AVRO文件并转换为parquet格式
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		String basicDir = "hdfs://archtorm:9000";
		Path schemaPath = new Path(basicDir + "/user/xuwenjuan/mapreduce/user.avsc");
		Path inputPath = new Path(basicDir + "/user/xuwenjuan/mapreduce/user.avro");
		Path outputPath = new Path(basicDir + "/user/xuwenjuan/mapreduce/output_parquet");
		Configuration conf = new Configuration();
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		Job job = new Job(conf);
		job.setJarByClass(Avro2ParquetMapReduce.class);

		FileSystem fs = FileSystem.get(conf);
		InputStream in = fs.open(schemaPath);
		Schema avroSchema = new Schema.Parser().parse(in);

		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(AvroKeyInputFormat.class);

		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setOutputPath(job, outputPath);
		AvroParquetOutputFormat.setSchema(job, avroSchema);
		AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		AvroParquetOutputFormat.setCompressOutput(job, true);

		AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);

		job.setMapperClass(Avro2ParquetMapper.class);
		job.setNumReduceTasks(0);

		job.waitForCompletion(true);
	}
}
