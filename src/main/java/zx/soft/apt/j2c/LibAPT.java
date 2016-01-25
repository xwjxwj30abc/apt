package zx.soft.apt.j2c;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.apt.j2c.AptLibrary.apt_stream;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

/**
 * 测试文件还原
 * @author fgq
 *
 */
public class LibAPT {

	private static Logger logger = LoggerFactory.getLogger(LibAPT.class);

	public static void getPcapFile(String[] args) throws IOException, InterruptedException {
		int ret = 0;
		boolean run = true;
		int len;
		int index = 0;
		int looptime = 0;
		String lib_dir = "/home/fgq/Desktop/apt.so/apt_ubuntu_so_20160125/lib_dir";
		String output_dir = "/home/fgq/output";
		PointerByReference output = new PointerByReference();
		File pacpFile = new File("/home/fgq/develop/eclipse/work/apt/src/main/resources/file");
		List<File> files = new ArrayList<>();
		if (pacpFile.isDirectory()) {
			File[] tempFiles = pacpFile.listFiles();
			if (tempFiles != null && tempFiles.length > 0) {
				files = Arrays.asList(tempFiles);
			}
		} else {
			files.add(pacpFile);
		}

		InputStream inputStream = null;
		byte[] buf = new byte[4096];
		Pointer pointer = new Memory(buf.length);

		//初始化apt_stream
		apt_stream stream = AptLibrary.INSTANCE.apt_stream_init(lib_dir, output_dir);
		if (stream == null) {
			logger.warn("stream 初始化为空");
			AptLibrary.INSTANCE.apt_stream_destroy(stream);
			System.exit(0);
		}

		logger.info("index=" + index);
		File file = files.get(index);
		inputStream = new FileInputStream(file);
		index++;
		inputStream.skip(24);

		while (run) {
			//apt_stream_loop返回APT_ALERT有延时，可能pcap文件内容已经输入完apt_stream_loop还没有返回APT_ALERT
			//这时需要等待几秒再调用一次apt_stream_loop查看结果
			//Thread.sleep(500);
			ret = AptLibrary.INSTANCE.apt_stream_loop(stream, output);
			switch (ret) {
			case AptLibrary.APT_OK:
				break;
			case AptLibrary.APT_NEED_MORE:
				len = inputStream.read(buf, 0, buf.length);
				if (len == -1) {
					inputStream.close();
					if (index == files.size()) {
						index = 0;
						looptime++;
						//						if (looptime == 10) {
						//							System.exit(0);
						//						}
						System.err.println("循环次数:looptime=" + looptime);
					}
					inputStream = new FileInputStream(files.get(index));
					index++;
					inputStream.skip(24);
					len = inputStream.read(buf, 0, buf.length);
				}
				if (len > 0) {
					pointer.write(0, buf, 0, buf.length);
					if (AptLibrary.INSTANCE.apt_stream_push(stream, pointer, len) != 0) {
						logger.warn("fail to call apt_stream_push");
						AptLibrary.INSTANCE.apt_stream_destroy(stream);
						System.exit(0);
					}
				}
				break;
			case AptLibrary.APT_ALERT:
				Pointer alertPointer = output.getValue();
				show(alertPointer);
				long idAlert = alertPointer.getLong(64);
				logger.info("id=" + idAlert);
				AptLibrary.INSTANCE.apt_stream_output_free(output.getValue(), ret);
				break;
			case AptLibrary.APT_FILE:
				Pointer afPointer = output.getValue();
				show(afPointer);
				Pointer fileP = afPointer.getPointer(64);
				logger.info("file name=" + fileP.getString(0));
				AptLibrary.INSTANCE.apt_stream_output_free(output.getValue(), ret);
				break;
			case AptLibrary.APT_ERR_BUF:
				run = false;
				break;
			default:
				logger.error("unknown apt_stream_loop return value" + ret);
				run = false;
				break;
			}
		}

		inputStream.close();
		AptLibrary.INSTANCE.apt_stream_destroy(stream);
		return;
	}

	private static void show(Pointer pointer) throws UnknownHostException {
		long ts = pointer.getLong(0);
		byte type = pointer.getByte(8);
		byte protocol = pointer.getByte(9);
		short service = pointer.getShort(10);
		int len = pointer.getInt(24);
		logger.info("ts=" + ts);
		logger.info("type=" + type);
		logger.info("protocol=" + protocol);
		logger.info("service=" + service);
		logger.info("len=" + len);

		Pointer un = pointer.share(28);
		byte[] source = un.getByteArray(0, 4);
		byte[] des = un.getByteArray(4, 4);
		byte[] sport = un.getByteArray(8, 2);
		byte[] dport = un.getByteArray(10, 2);
		logger.info("source ip=" + Inet4Address.getByAddress(source) + "");
		logger.info("des ip=" + Inet4Address.getByAddress(des) + "");
		logger.info("source port=" + ((sport[1] & 0xFF) | (sport[0] & 0xFF) << 8) + "");
		logger.info("des port=" + ((dport[1] & 0xFF) | (dport[0] & 0xFF) << 8) + "");

	}

	public static void main(String[] args) throws IOException, InterruptedException {
		LibAPT.getPcapFile(args);
	}

}
