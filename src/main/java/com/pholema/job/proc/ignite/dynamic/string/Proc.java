package com.pholema.job.proc.ignite.dynamic.string;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.pholema.job.proc.ProcImpl;
import com.pholema.job.utils.StringUtil;
import com.pholema.tool.utils.db.DbUtils;
import com.pholema.tool.utils.dynamic.PropertiesUtils;

public class Proc implements ProcImpl {
	final static Logger logger = Logger.getLogger(Proc.class);
	private static Store store;
	private static Gson gson = new Gson();
	private static boolean isClearCache = PropertiesUtils.properties.getProperty("clear.cache") != null && PropertiesUtils.properties.getProperty("clear.cache").equalsIgnoreCase("true");

	@Override
	public void init() {
		store = new Store();
		store.init(PropertiesUtils.properties.getProperty("ignite.conf"));
		if (isClearCache) {
			logger.info("clearing cache");
			store.clear();
			logger.info("clearing completed");
		}
	}

	@Override
	public void run() {
		String dbServer = PropertiesUtils.properties.getProperty("db.server");
		String dbDatabase = PropertiesUtils.properties.getProperty("db.database");
		String dbUsername = PropertiesUtils.properties.getProperty("db.username");
		String dbPassword = PropertiesUtils.properties.getProperty("db.password");
		String dbSQL = PropertiesUtils.properties.getProperty("db.sql");
		String keyColumnNames = PropertiesUtils.properties.getProperty("key.column.names");
		String igniteConf = PropertiesUtils.properties.getProperty("ignite.conf");
		String igniteCacheName = PropertiesUtils.properties.getProperty("ignite.cache.name");
		logger.info("dbServer:" + dbServer);
		logger.info("dbDatabase:" + dbDatabase);
		logger.info("dbUsername:" + dbUsername);
		logger.info("dbSQL:" + dbSQL);
		logger.info("igniteConf:" + igniteConf);
		logger.info("igniteCacheName:" + igniteCacheName);
		load(dbServer, dbDatabase, dbUsername, dbPassword, dbSQL, keyColumnNames);
	}

	@Override
	public void quit() {
		store.destroy();
	}

	public static void load(String server, String database, String username, String password, String sql, String keyColumnNames) {
		try {
			HashMap<String, String> map = new HashMap<String, String>();
			List<String> keyNameList = Arrays.asList(keyColumnNames.split(","));

			ResultSet res = DbUtils.load(server, database, username, password, sql);
			ResultSetMetaData resultSetMetaData = res.getMetaData();
			int cc = 0;
			while (res.next()) {
				String key = "";
				for (String keyName : keyNameList) {
					if (res.getString(keyName) != null) {
						if (!key.equals(""))
							key += "_";
						key += res.getString(keyName).trim();
					}
				}

				JsonObject object = StringUtil.sqlResultToJsonObject(res, resultSetMetaData);

				if (!key.equals("")) {
					map.put(key.toLowerCase(), gson.toJson(object));
					if (cc < 10)
						logger.info("key:" + key.toLowerCase() + ",value:" + gson.toJson(object));
					cc++;
				}
			}

			logger.info("map saize:" + map.size());
			// save to ignite
			store.put(map);

		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			logger.error(sw.toString());
		}
	}

}
