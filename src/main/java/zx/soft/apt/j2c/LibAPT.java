package zx.soft.apt.j2c;

import java.io.BufferedInputStream;
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
		int getFileNum = 0;
		String lib_dir = "/home/fgq/Desktop/apt_ubuntu_so/lib_dir";
		String output_dir = "/home/fgq/apt_output";
		PointerByReference output = new PointerByReference();
		File pacpFile = new File("src/main/resources/file");
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

		inputStream = new BufferedInputStream(new FileInputStream(files.get(index++)));
		inputStream.skip(24);

		while (run) {
			//apt_stream_loop返回APT_ALERT有延时，可能pcap文件内容已经输入完apt_stream_loop还没有返回APT_ALERT
			//这时需要等待几秒再调用一次apt_stream_loop查看结果
			//Thread.sleep(100);
			ret = AptLibrary.INSTANCE.apt_stream_loop(stream, output);
			switch (ret) {
			case AptLibrary.APT_OK:
				break;
			case AptLibrary.APT_NEED_MORE:
				len = inputStream.read(buf, 0, buf.length);
				if (len == -1) {
					if (index == files.size()) {
						logger.info("还原文件数:" + getFileNum);
						System.exit(0);
					}
					inputStream = new BufferedInputStream(new FileInputStream(files.get(index++)));
					inputStream.skip(24);
					logger.info("当前文件index是" + index);
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
				logger.info("apt_alert信息：");
				show(alertPointer);
				long idAlert = pointer.getLong(64);
				logger.info("id=" + idAlert);
				AptLibrary.INSTANCE.apt_stream_output_free(output.getValue(), ret);
				break;
			case AptLibrary.APT_FILE:
				getFileNum++;
				logger.info("apt_file信息：");
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
		long tsAlert = pointer.getLong(0);
		byte typeAlert = pointer.getByte(8);
		byte protocolAlert = pointer.getByte(9);
		short serviceAlert = pointer.getShort(10);
		int lenAlert = pointer.getInt(24);
		logger.info("*****************begin***************");
		logger.info("ts=" + tsAlert);
		logger.info("type=" + typeAlert);
		logger.info("protocol=" + protocolAlert);
		logger.info("service=" + serviceAlert);
		logger.info("len=" + lenAlert);

		Pointer unAlert = pointer.share(28);
		byte[] sourceAlert = unAlert.getByteArray(0, 4);
		byte[] desAlert = unAlert.getByteArray(4, 4);
		byte[] sportAlert = unAlert.getByteArray(8, 2);
		byte[] dportAlert = unAlert.getByteArray(10, 2);
		logger.info("source ip=" + Inet4Address.getByAddress(sourceAlert) + "");
		logger.info("des ip=" + Inet4Address.getByAddress(desAlert) + "");
		logger.info("source port=" + ((sportAlert[1] & 0xFF) | (sportAlert[0] & 0xFF) << 8) + "");
		logger.info("des port=" + ((dportAlert[1] & 0xFF) | (dportAlert[0] & 0xFF) << 8) + "");
		logger.info("*****************end***************");
		logger.info("");
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		LibAPT.getPcapFile(args);
	}

}
