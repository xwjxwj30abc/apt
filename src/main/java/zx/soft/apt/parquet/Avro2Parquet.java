package zx.soft.apt.parquet;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import com.google.common.io.Resources;

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

	public void testAvro2Parquet() {

	}

	public static void main(String[] args) throws IOException {
		Schema schema = new Schema.Parser().parse(Resources.getResource("user.avsc").openStream());
		File fileFrom = new File("src/main/resources/user.avro");
		File fileTo = new File("src/main/resources/user.parquet");
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(fileFrom, datumReader);
		AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(new Path(fileTo.getPath()),
				schema);
		GenericRecord user = null;
		while (dataFileReader.hasNext()) {
			user = dataFileReader.next();
			System.out.println(user);
			writer.write(user);
		}
		dataFileReader.close();
		writer.close();
	}
}
