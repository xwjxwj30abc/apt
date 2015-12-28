package zx.soft.apt.j2c;

import java.nio.ByteBuffer;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

/**
 * JNA Wrapper for library <b>test</b><br>
 * This file was autogenerated by <a href="http://jnaerator.googlecode.com/">JNAerator</a>,<br>
 * a tool written by <a href="http://ochafik.com/">Olivier Chafik</a> that <a href="http://code.google.com/p/jnaerator/wiki/CreditsAndLicense">uses a few opensource projects.</a>.<br>
 * For help, please visit <a href="http://nativelibs4java.googlecode.com/">NativeLibs4Java</a> , <a href="http://rococoa.dev.java.net/">Rococoa</a>, or <a href="http://jna.dev.java.net/">JNA</a>.
 */
public interface SampleLibrary extends Library {
	public static final String JNA_LIBRARY_NAME = "sample";
	public static final NativeLibrary JNA_NATIVE_LIB = NativeLibrary.getInstance(SampleLibrary.JNA_LIBRARY_NAME);
	public static final SampleLibrary INSTANCE = (SampleLibrary) Native.loadLibrary(SampleLibrary.JNA_LIBRARY_NAME,
			SampleLibrary.class);

	/**
	 * Original signature : <code>void example2_sendString(const char*)</code><br>
	 * <i>native declaration : line 2</i><br>
	 * @deprecated use the safer methods {@link #example2_sendString(java.lang.String)} and {@link #example2_sendString(com.sun.jna.Pointer)} instead
	 */
	@Deprecated
	void example2_sendString(Pointer pszVal);

	/**
	 * Original signature : <code>void example2_sendString(const char*)</code><br>
	 * <i>native declaration : line 2</i>
	 */
	void example2_sendString(String pszVal);

	/**
	 * Original signature : <code>void example2_getString(char**)</code><br>
	 * <i>native declaration : line 3</i>
	 */
	void example2_getString(PointerByReference ppszVal);

	/**
	 * Original signature : <code>void example2_cleanup(char*)</code><br>
	 * <i>native declaration : line 4</i><br>
	 * @deprecated use the safer methods {@link #example2_cleanup(java.nio.ByteBuffer)} and {@link #example2_cleanup(com.sun.jna.Pointer)} instead
	 */
	@Deprecated
	void example2_cleanup(Pointer pszVal);

	/**
	 * Original signature : <code>void example2_cleanup(char*)</code><br>
	 * <i>native declaration : line 4</i>
	 */
	void example2_cleanup(ByteBuffer pszVal);
}
