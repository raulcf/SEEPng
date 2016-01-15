package uk.ac.imperial.lsds.seep.api.data;

import java.nio.charset.Charset;

public interface SchemaParser {
	public bytes[] bytesFromString(String);
	public String stringFromBytes(byte[]);
	public Charset getCharset();
	public void setCharset(Charset);
}
