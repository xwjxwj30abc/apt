package zx.soft.apt.j2c;

import zx.soft.apt.j2c.AptLibrary.apt_alter;
import zx.soft.apt.j2c.AptLibrary.apt_conntrack;
import zx.soft.apt.j2c.AptLibrary.apt_file;
import zx.soft.apt.j2c.AptLibrary.apt_stream;

import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class LibAPT {

	public static void main(String[] args) {

		String lib_dir = "/home/fgq/develop/eclipse/work/apt/src/main/resources";
		String output_dir = "/home/fgq/Desktop/ADT_OUTPUT";
		apt_stream stream = AptLibrary.INSTANCE.apt_stream_init(lib_dir, output_dir);
		System.out.println(stream);
		int ret;
		PointerByReference output = new PointerByReference();
		while (true) {
			ret = AptLibrary.INSTANCE.apt_stream_loop(stream, output);
			switch (ret) {
			case AptLibrary.APT_OK:
				break;
			case AptLibrary.APT_NEED_MORE:
				//添加更多数据数据
				String buf = null;
				int len = 4096;
				int success = 1;
				success = AptLibrary.INSTANCE.apt_stream_push(stream, buf, len);
				if (success == 0) {
					System.out.println("push数据成功");
				} else {
					System.out.println("push失败，重试一次");
					success = AptLibrary.INSTANCE.apt_stream_push(stream, buf, len);
				}
				break;
			case AptLibrary.APT_FILE:
				Pointer filePointer = output.getValue();
				apt_file file = new apt_file(filePointer);
				file.read();
				System.out.println(file.filename);
				AptLibrary.INSTANCE.apt_stream_output_free(filePointer, ret);
				break;
			case AptLibrary.APT_CONNTRACK:
				Pointer conntrackPointer = output.getValue();
				apt_conntrack conntrack = new apt_conntrack(conntrackPointer);
				conntrack.read();
				System.out.println(conntrack.ts);
				break;
			case AptLibrary.APT_ALTER:
				Pointer alertPointer = output.getPointer();
				apt_alter alter = new apt_alter(alertPointer);
				alter.read();
				System.out.println(alter.id);
				break;
			default:
				break;
			}
		}

	}
}
