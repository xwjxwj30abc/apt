package zx.soft.apt.hbase;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.client.Result;

import zx.soft.apt.utis.MD5Filter;
import zx.soft.hbase.api.core.HBaseTable;
import zx.soft.hbase.api.core.HConn;

public class MainTestClass {
	public static void main(String[] args) throws NoSuchAlgorithmException {

		try {
			HBaseTable h = new HBaseTable(HConn.getHConnection(), "kafka_db");
			Result r = h.get("000000000000000000000000ac10441200000000565afc27");
			String fileName = "src/main/resources/test_tmp.cap";
			Combine.conbinbyte2File("load", r, fileName);
			String v = MD5Filter.getMd5ByFile(new File("src/main/resources/test.cap"));
			String v1 = MD5Filter.getMd5ByFile(new File(fileName));
			System.out.println(v.toUpperCase());
			System.out.println(v1.toUpperCase());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
