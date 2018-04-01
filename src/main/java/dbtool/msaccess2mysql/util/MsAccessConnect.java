package dbtool.msaccess2mysql.util;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Table;

public class MsAccessConnect {
	private Database db;
	private String msaccessDb;
	
	public MsAccessConnect(String msaccessDb) {
		super();
		this.msaccessDb = msaccessDb;
	}

	public void connect() throws IOException {
		File dbFile = new File(msaccessDb);
		if (dbFile.exists() && !dbFile.isDirectory()) {
			db = DatabaseBuilder.open(dbFile);
		}
	}
	
	public Set<String> getTableNames() throws IOException {
		return db.getTableNames();
	}
	
	public Table getTable(String tblname) throws IOException {
		return db.getTable(tblname);
	}
	
	
	
	public void close() throws IOException {
		db.close();
	}
}
