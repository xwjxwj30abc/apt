package zx.soft.apt.hbase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;

public class Combine {

	public static void conbinbyte2File(String family, Result result, String fileName) throws FileNotFoundException {
		//byte[] destAray = null;
		File file = new File(fileName);
		FileOutputStream st = new FileOutputStream(file);
		//ByteArrayOutputStream bos = new ByteArrayOutputStream();
		NavigableMap<byte[], byte[]> map = result.getFamilyMap(family.getBytes());
		try {
			for (int i = 1; i < map.size(); i++) {
				byte[] tmp = map.get(String.valueOf(i).getBytes());
				st.write(tmp);
				//bos.write(tmp);
			}
			//bos.flush();
			st.flush();
			//destAray = bos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				st.close();
				//	bos.close();
			} catch (IOException e) {
			}
		}
	}
}
