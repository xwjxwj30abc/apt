package zx.soft.apt.j2c;

import zx.soft.apt.j2c.AptLibrary.apt_stream;

import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class TestInitLib {

	public static void main(String[] args) {

		String lib_dir = "/home/fgq/develop/eclipse/work/apt/src/main/resources";
		String output_dir = "/home/fgq/Desktop/ADT_OUTPUT";
		apt_stream stream = AptLibrary.INSTANCE.apt_stream_init(lib_dir, output_dir);
		System.out.println(stream);
		SampleLibrary.INSTANCE.example2_sendString("hehe");
		PointerByReference ptrRef = new PointerByReference();
		SampleLibrary.INSTANCE.example2_getString(ptrRef);
		Pointer p = ptrRef.getValue();
		String val = p.getString(0);
		System.out.println(val);
		SampleLibrary.INSTANCE.example2_cleanup(p);
	}
}
