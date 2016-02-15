package uk.ac.imperial.lsds.seep.api.data;

public interface SchemaParser {
	public byte[] bytesFromString(String textRecord);
	public String stringFromBytes(byte[] binaryRecord);
	public String getCharsetName();
	public void setCharset(String newencoding);
}
