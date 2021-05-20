package com.pholema.job.proc.ignite.dynamic.persist.to.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.cache.Cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.log4j.Logger;

import com.pholema.job.proc.ProcImpl;
import com.pholema.tool.starter.ignite.IgniteStore;
import com.pholema.tool.utils.common.DateUtils;
import com.pholema.tool.utils.common.StringUtils;
import com.pholema.tool.utils.dynamic.PropertiesUtils;

public class Proc implements ProcImpl {

	final static Logger logger = Logger.getLogger(Proc.class);

	@Override
	public void init() {
	}

	@Override
	public void run() {

		String cacheName = PropertiesUtils.properties.getProperty("cacheName");
		String className = PropertiesUtils.properties.getProperty("className");
		String whereStatement = PropertiesUtils.properties.getProperty("whereStatement");

		Class<?> mClass;
		try {
			mClass = Class.forName(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return;
		}
		IgniteStore igniteStore = new IgniteStore();
		igniteStore.init();
		IgniteCache cache = igniteStore.getCache(cacheName);
		// List<Object> list;
		Map<Object, Object> map = new HashMap<>();

		try {
			String todayString_Start = DateUtils.getDateTime_day(0, "yyyy-MM-dd 00:00:00");
			logger.info("todayString_Start " + todayString_Start);
			whereStatement = whereStatement.replace("$startTime", todayString_Start);
			map = igniteQuery(cache, mClass, whereStatement);
			saveToFile(map, cache.getName());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void quit() {

	}

	/**
	 * query key and values from ignite cache
	 * 
	 * @param cache
	 * @param mClass
	 * @param whereStatement
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	private <V, M> Map<Object, Object> igniteQuery(IgniteCache<V, M> cache, Class<M> mClass, String whereStatement) throws SQLException, IOException {
		// List<Object> list = new ArrayList<>();
		Map<Object, Object> map = new HashMap<>();
		String sql = whereStatement;
		logger.info("sql " + sql);
		QueryCursor query = cache.query(new SqlQuery<>(mClass, sql));
		Iterator iter = query.iterator();
		while (iter.hasNext()) {
			// list.add(((Cache.Entry) iter.next()).getValue());
			Cache.Entry entry = (Cache.Entry)iter.next();
			map.put(entry.getKey(), entry.getValue());
		}
		logger.info(cache.getName() + " --> got " + map.size() + " rows.");
		return map;
	}

	/**
	 * data format: key|GSON(value)
	 * 
	 * @param map
	 * @param cacheName
	 * @return
	 * @throws IOException
	 */
	synchronized private static boolean saveToFile(Map<Object, Object> map, String cacheName) throws IOException {
		boolean rtn = false;
		String nowDateStr = DateUtils.toDateStrYYYYMMDD(new Date());
		String filePath = PropertiesUtils.properties.getProperty("app.home") + "/" + cacheName + nowDateStr + ".bak";
		File file = new File(filePath);

		if (!file.exists()) {
			file.createNewFile();
		} else { // clear file
			PrintWriter clear = new PrintWriter(file);
			clear.print("");
			clear.flush();
			clear.close();
		}

		PrintWriter writer = new PrintWriter(new FileOutputStream(file, true));
		for (Object o : map.keySet()) {
			String result = o.toString() + "|" + StringUtils.toGson(map.get(o)) + "\r\n";
			writer.write(result);
			writer.flush();
		}
		writer.close();
		rtn = true;
		return rtn;
	}

}
