package zx.soft.apt.j2c;

import com.sun.jna.Library;
import com.sun.jna.Native;

public class HelloWorld {

	public interface CLibrary extends Library {
		CLibrary INSTANCE = (CLibrary) Native.loadLibrary("test", CLibrary.class);

		int add(int a, int b);
	}

	public static void main(String[] args) {
		int c = CLibrary.INSTANCE.add(10, 2);
		System.out.println(c);
	}

}
