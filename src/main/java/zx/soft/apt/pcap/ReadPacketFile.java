package zx.soft.apt.pcap;

import java.io.EOFException;
import java.util.concurrent.TimeoutException;

import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapHandle.TimestampPrecision;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.Packet;

public class ReadPacketFile {

	private static final int COUNT = 5;

	public static void main(String[] args) throws PcapNativeException, NotOpenException {
		String PCAP_FILE = "src/main/resources/test.cap";
		PcapHandle handle;
		try {
			handle = Pcaps.openOffline(PCAP_FILE, TimestampPrecision.NANO);
		} catch (PcapNativeException e) {
			handle = Pcaps.openOffline(PCAP_FILE);
		}

		for (int i = 0; i < COUNT; i++) {
			try {
				Packet packet = handle.getNextPacketEx();
				System.out.println(handle.getTimestamp());
				System.out.println(packet);
			} catch (TimeoutException e) {
			} catch (EOFException e) {
				System.out.println("EOF");
				break;
			}
		}

		handle.close();
	}

}
