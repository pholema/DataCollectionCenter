package com.pholema.job.proc.hbase.dynamic;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.pholema.job.proc.ProcImpl;
import com.pholema.job.utils.StringUtil;
import com.pholema.tool.starter.hbase.dao.HBaseClient;
import com.pholema.tool.utils.common.HashUtils;
import com.pholema.tool.utils.dynamic.PropertiesUtils;

public class Proc implements ProcImpl {
	final static Logger logger = Logger.getLogger(Proc.class);
	protected static Gson gson = new Gson();

	protected static String tableName = PropertiesUtils.properties.getProperty("hbase.tableName");
	protected static String columnFamily = PropertiesUtils.properties.getProperty("hbase.columnFamily");
	protected static String columnNames = PropertiesUtils.properties.getProperty("hbase.column.dbColumnNames");
	protected static String rowkeyNames = PropertiesUtils.properties.getProperty("hbase.rowkey.dbColumnNames");

	protected static String columnNamesFixed = PropertiesUtils.properties.getProperty("hbase.column.dbColumnNames.fixed");

	@Override
	public void init() {
		HBaseClient.getInstance();
	}

	@Override
	public void run() {
		String dbServer = PropertiesUtils.properties.getProperty("db.server");
		String dbDatabase = PropertiesUtils.properties.getProperty("db.database");
		String dbUsername = PropertiesUtils.properties.getProperty("db.username");
		String dbPassword = PropertiesUtils.properties.getProperty("db.password");
		String dbSQL = PropertiesUtils.properties.getProperty("db.sql");

		load(dbServer, dbDatabase, dbUsername, dbPassword, dbSQL);
	}

	@Override
	public void quit() {
	}

	protected void load(String server, String database, String username, String password, String sql) {
		try {
			Map<String, Map<String, String>> map_string = new HashMap<>();
			Map<String, Map<String, JsonArray>> map_obj = new HashMap<>();
			List<String> rowkeyList = Arrays.asList(rowkeyNames.split(","));

			ResultSet res = myLoad(server, database, username, password, sql);
			ResultSetMetaData resultSetMetaData = res.getMetaData();
			int cc = 0;
			while (res.next()) {
				String rowkey = getResultValue4columnNames(res, rowkeyList);
				String columnkey = "";
				if(columnNamesFixed!=null && columnNamesFixed.trim().length()>0){
					columnkey = columnNamesFixed;
				}else{
					List<String> columnkeyList = Arrays.asList(columnNames.split(","));
					columnkey = getResultValue4columnNames(res, columnkeyList);
				}
					

				if (!rowkey.equals("") && !columnkey.equals("")) {
					JsonObject object = StringUtil.sqlResultToJsonObject(res, resultSetMetaData);

					Map<String, JsonArray> map_sub = map_obj.get(rowkey);
					if (map_sub == null) {
						map_sub = new HashMap<>();
						map_obj.put(rowkey, map_sub);
					}
					JsonArray values = map_sub.get(columnkey);
					if (values == null) {
						values = new JsonArray();
						map_sub.put(columnkey, values);
					}
					values.add(object);
				} else {
					logger.warn("key insufficient rowkey:" + rowkey + ",columnkey:" + columnkey);
				}
			}

			for (String rowkey : map_obj.keySet()) {
				Map<String, String> map_sub = new HashMap<>();
				for (String columnkey : map_obj.get(rowkey).keySet()) {
					String value = gson.toJson(map_obj.get(rowkey).get(columnkey));
					map_sub.put(columnkey, value);
					if (cc < 10)
						logger.info("rowkey:" + rowkey + "(" + HashUtils.md5(rowkey) + ")" + ",value:" + value);
					cc++;
				}
				map_string.put(HashUtils.md5(rowkey), map_sub);
			}

			logger.info("map saize:" + map_string.size());
			// save to hbase
			HBaseClient.getInstance().postStringValueByMultiColumn(tableName, columnFamily, map_string);

		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			logger.error(sw.toString());
		}
	}

	protected String getResultValue4columnNames(ResultSet res, List<String> keynameList) throws SQLException {
		String key = "";
		for (String keyName : keynameList) {
			if (res.getString(keyName) != null) {
				if (!key.equals(""))
					key += "_";
				key += res.getString(keyName).trim().toLowerCase();
			}
		}
		return key;
	}

	protected ResultSet myLoad(String server, String database, String username, String password, String sql) {
		try {
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			String sqlserver_connect_string = "jdbc:jtds:sqlserver://" + server + ":1433/" + database;
			Connection connMSSQL;
			connMSSQL = DriverManager.getConnection(sqlserver_connect_string, username, password);
			PreparedStatement stmt = connMSSQL.prepareStatement(sql);
			setSQLStatement(stmt);
			return stmt.executeQuery();
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			logger.error(sw.toString());
			return null;
		}
	}

	/**
	 * do something customized Statement setting, example: java.sql.Timestamp timestampStart = new java.sql.Timestamp(DateUtils.getDateStart(startOffset).getTime()); stmt.setTimestamp(1, timestampStart);
	 * 
	 * @param stmt
	 */
	protected void setSQLStatement(PreparedStatement stmt) throws SQLException {
	}

}
