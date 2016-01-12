package zx.soft.apt.j2c;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.apt.hbase.PcapHBase;

public class CombineToFile {

	private static Logger logger = LoggerFactory.getLogger(CombineToFile.class);

	/**
	 * 获取kafka中apt-cache消息并保存能完整恢复的pcap文件
	 * @throws IOException
	 */
	public static void saveToFile() throws IOException {
		PcapHBase hbase = new PcapHBase("kafka_db");
		String family = "apt-cache";
		String filePre = "src/main/resources/file/";
		Result result = null;
		FileOutputStream st = null;
		//		String startRow = "1452585600";
		//		String stopRow = "2452596400";
		HTableInterface table = hbase.getTable();
		Scan scan = new Scan();
		//		scan.setStartRow(startRow.getBytes());
		//		scan.setStopRow(stopRow.getBytes());
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> iterator = scanner.iterator();
		while (iterator.hasNext()) {
			result = iterator.next();
			String key = Bytes.toString(result.getRow());
			logger.info("当前rowkey:" + key);
			NavigableMap<byte[], byte[]> map = result.getFamilyMap(family.getBytes());
			byte[] size = map.get(Bytes.toBytes(String.valueOf(65535)));
			//若存在文件结束标志，则表示文件已传输完毕，可能由于kafka消耗的问题，导致文件未真正接收完整
			if (size != null) {
				logger.info("获得文件结束标志并得到文件总长度；" + "rowkey: " + key + "; expext:" + Integer.valueOf(new String(size))
						+ "; fact:" + map.size());
				if (Integer.valueOf(new String(size)) == (map.size() - 1)) {
					logger.info("表的列数与文件大小一致，此时：" + "rowkey: " + key + "; expext:" + Integer.valueOf(new String(size))
							+ "; fact:" + map.size());
					st = new FileOutputStream(filePre + key.substring(0, 10) + "_"
							+ key.substring(11).replace('.', '_') + ".pcap");
					for (int i = 0; i < Integer.valueOf(new String(size)); i++) {
						byte[] tmp = map.get(Bytes.toBytes(String.valueOf(i)));
						if (tmp != null) {
							st.write(tmp);
						}
					}
					st.flush();
					st.close();
				}
			}
		}

	}

	public static void main(String[] args) throws IOException {
		CombineToFile.saveToFile();
	}
}
