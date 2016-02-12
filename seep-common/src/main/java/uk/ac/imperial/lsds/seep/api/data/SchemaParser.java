package uk.ac.imperial.lsds.seep.api.data;

import java.nio.charset.Charset;

public interface SchemaParser {
	public byte[] bytesFromString(String textRecord);
	public String stringFromBytes(byte[] binaryRecord);
	public Charset getCharset();
	public void setCharset(Charset newencoding);
}
