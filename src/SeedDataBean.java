
public class SeedDataBean {

	private String name;
	private int level;
	private String rowKey;
	private String SGUID;
	private String fileName;

	public SeedDataBean(String fileName, String name, int level, String rowKey, String SGUID) {
		this.name = name;
		this.level = level;
		this.rowKey = rowKey;
		this.SGUID = SGUID;
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public String getSGUID() {
		return SGUID;
	}

	public void setSGUID(String sGUID) {
		SGUID = sGUID;
	}

	@Override
	public String toString() {
		return this.fileName+"::"+this.level + "::" + this.name + "::" + this.rowKey + "::" + this.SGUID;
	}

}
