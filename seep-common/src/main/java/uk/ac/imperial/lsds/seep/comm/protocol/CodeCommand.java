package uk.ac.imperial.lsds.seep.comm.protocol;

public class CodeCommand implements CommandType {

	private int dataSize;
	private byte[] data;
	
	private short queryType;
	private String baseClassName;
	private String[] queryConfig;
	private String methodName;

	public CodeCommand(){}
	
	public CodeCommand(short queryType, byte[] data, String baseClassName, String[] queryConfig, String methodName) {
		this.queryType = queryType;
		this.dataSize = data.length;
		this.data = data;
		this.baseClassName = baseClassName;
		this.queryConfig = queryConfig;
		this.methodName = methodName;
	}

	@Override
	public short type() {
		return MasterWorkerProtocolAPI.CODE.type();
	}
	
	public int getDataSize(){
		return dataSize;
	}
	
	public byte[] getData(){
		return data;
	}
	
	public String getBaseClassName() {
		return baseClassName;
	}
	
	public String[] getQueryConfig() {
		return queryConfig;
	}
	
	public String getMethodName() {
		return methodName;
	}
	
	public short getQueryType() {
		return queryType;
	}
	
}
