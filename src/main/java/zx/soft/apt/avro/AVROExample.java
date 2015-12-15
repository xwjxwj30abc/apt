package zx.soft.apt.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class AVROExample {

	public static void main(String[] args) throws IOException {

		Schema schema = new Schema.Parser().parse(new File("src/main/resources/user.avsc"));
		File file = new File("src/main/resources/user.avro");
		GenericRecord user1 = new GenericData.Record(schema);

		user1.put("name", "Ben");
		user1.put("favorite_number", 7);

		GenericRecord user2 = new GenericData.Record(schema);
		user2.put("name", "Bob");
		user2.put("favorite_number", 8);
		user2.put("favorite_color", "pink");

		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.close();

		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
		GenericRecord user = null;
		while (dataFileReader.hasNext()) {
			user = dataFileReader.next();
			System.out.println(user);
		}
		dataFileReader.close();
	}
}
