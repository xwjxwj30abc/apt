package zx.soft.apt.parquet;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import com.google.common.io.Resources;

/**
 * avro文件转换为parquet文件
 * @author fgq
 *
 */
public class Avro2Parquet {

	public static boolean testAvroSchemaConverter() throws IOException {
		Schema avroSchema = new Schema.Parser().parse(Resources.getResource("user.avsc").openStream());
		AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();
		MessageType schema = avroSchemaConverter.convert(avroSchema);
		String schemaString = "message User {" + "required binary name (UTF8);" + "optional int32 favorite_number;"
				+ "optional binary favorite_color (UTF8);}";
		System.out.println(schema.toString());
		MessageType expectedMT = MessageTypeParser.parseMessageType(schemaString);
		return expectedMT.equals(schema);
	}

	public static void main(String[] args) throws IOException {
		Schema schema = new Schema.Parser().parse(Resources.getResource("user.avsc").openStream());
		File fileTo = new File("src/main/resources/user.parquet");
		AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(new Path(fileTo.getPath()),
				schema);
		GenericRecord user1 = new GenericData.Record(schema);
		user1.put("name", "Bengadf");
		user1.put("favorite_number", 7);
		GenericRecord user2 = new GenericData.Record(schema);
		user2.put("name", "Bobdafg");
		user2.put("favorite_number", 8);
		user2.put("favorite_color", "pinkdfg");
		writer.write(user1);
		writer.write(user2);
		writer.close();

	}
}
