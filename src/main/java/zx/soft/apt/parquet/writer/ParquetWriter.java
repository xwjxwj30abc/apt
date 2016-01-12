package zx.soft.apt.parquet.writer;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetWriter {

	private final Logger logger = LoggerFactory.getLogger(ParquetWriter.class);
	private static final String BASICDIR = "hdfs://192.168.3.12:9000";
	private AvroParquetWriter<GenericRecord> writer;
	private Schema schema;
	private Configuration conf = new Configuration();

	public ParquetWriter(String dataDir, String schemaPath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		schema = new Schema.Parser().parse(fs.open(new Path(schemaPath)));
		File file = new File(dataDir + "/" + System.currentTimeMillis() + ".parquet");
		writer = new AvroParquetWriter(new Path(file.getPath()), schema);
		logger.info("创建ParquetWriter成功");
	}

	public void write(GenericRecord record) {
		try {
			writer.write(record);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public static void main(String[] args) throws IOException {

		String dataDir = "";
		String schemaDir = "";

		ParquetWriter writer = new ParquetWriter(dataDir, schemaDir);
		FileSystem fs = FileSystem.get(writer.conf);
		Path schemaPath = new Path(schemaDir);

		GenericRecord user1 = new GenericData.Record(writer.getSchema());
		user1.put("name", "Bengadf");
		user1.put("favorite_number", 7);
		GenericRecord user2 = new GenericData.Record(writer.getSchema());
		user2.put("name", "Bobdafg");
		user2.put("favorite_number", 8);
		user2.put("favorite_color", "pinkdfg");
		writer.write(user1);
		writer.write(user2);
		writer.close();
	}

}
