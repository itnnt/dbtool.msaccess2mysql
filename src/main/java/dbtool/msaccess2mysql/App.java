package dbtool.msaccess2mysql;

import java.util.Arrays;

import util.CommonUtil;

/**
 * Hello world!
 *
 */
public class App {

	public static void main(String[] args) {
		MsAccess2MySQL msAccess2MySQL = new MsAccess2MySQL(); 
//		msAccess2MySQL.synchData();
		String t = "Payment_All";
		CommonUtil util = new CommonUtil();
		msAccess2MySQL.createOrAlterTableThenTruncateBeforeInsert(t, "raw_" + util.removeSpecialChar(t) + "",
				Arrays.asList(new String[] {}), 
				Arrays.asList(new String[] {}));;
		msAccess2MySQL.closeAllConnections();
	}
}
