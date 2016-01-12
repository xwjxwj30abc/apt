package zx.soft.apt.hbase;

import java.io.IOException;

public class MainTestClass {

	public static void main(String[] args) throws IOException {
		SaveToHBase save = new SaveToHBase();
		save.save();
	}

}
