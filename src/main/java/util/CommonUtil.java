package util;

public class CommonUtil {
	public String removeSpecialChar(String colname) {
		colname =  colname.toLowerCase().replaceAll("[^a-zA-Z0-9]+","_");
		return colname.startsWith("_")? colname.replace("_", "") : colname;
	}
}
