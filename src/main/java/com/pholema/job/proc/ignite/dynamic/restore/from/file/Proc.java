package com.pholema.job.proc.ignite.dynamic.restore.from.file;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.IgniteCache;
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
		String kClassName = PropertiesUtils.properties.getProperty("className.key");
		String vClassName = PropertiesUtils.properties.getProperty("className.value");
		Class<?> kClass;
		Class<?> vClass;
		try {
			kClass = Class.forName(kClassName);
			vClass = Class.forName(vClassName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return;
		}
		IgniteStore igniteStore = new IgniteStore();
		igniteStore.init();
		IgniteCache cache = igniteStore.getCache(cacheName);
		Map<Object, Object> map = new HashMap<>();
		try {
			map = getObjectsFromBackupFile(cache.getName(), vClass);
			ignitePut(map, kClass, vClass, cache);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void quit() {

	}

	// 讀檔
	public static <V> Map<Object, Object> getObjectsFromBackupFile(String cacheName, Class<V> vClass) throws IOException {
		Map<Object, Object> map = new HashMap<>();
		String nowDateStr = DateUtils.toDateStrYYYYMMDD(new Date());
		String filePath = PropertiesUtils.properties.getProperty("app.home") + "/" + cacheName + nowDateStr + ".bak";
		logger.info("filePath=" + filePath);

		BufferedReader reader = new BufferedReader(new FileReader(filePath));
		String line;
		Object obj;
		while ((line = reader.readLine()) != null) {
			try {
				String key = line.substring(0, line.indexOf("|"));
				String value = line.substring(line.indexOf("|") + 1);
//				logger.info("key:" + key);
//				logger.info("value:" + value);
				obj = StringUtils.fromGson(value, vClass);
				map.put(key, obj);
			} catch (Exception e) {
				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				logger.error("getSoMergeFromBackupFile error" + sw.toString());
				logger.error("line:" + line);
			}
		}
		reader.close();
		return map;
	}

	public <K, V> void ignitePut(Map<Object, Object> map, Class<K> kClass, Class<V> vClass, IgniteCache<K, V> cache) {
		logger.info("ignitePut ..." + map.size());
		for (Object key : map.keySet()) {
			cache.put(kClass.cast(key), vClass.cast(map.get(key)));
		}
		logger.info("ignitePut finish.");
	}

}
