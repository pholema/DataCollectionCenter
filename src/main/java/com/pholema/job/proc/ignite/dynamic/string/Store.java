package com.pholema.job.proc.ignite.dynamic.string;
import java.util.HashMap;

import javax.cache.Cache;

import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pholema.tool.starter.ignite.IgniteStore;
import com.pholema.tool.utils.dynamic.PropertiesUtils;

public class Store extends IgniteStore {
	private static final Logger logger = LoggerFactory.getLogger(Store.class);
	private IgniteCache<String, String> cache;
	private String igniteCacheName = PropertiesUtils.properties.getProperty("ignite.cache.name");

	public void init(){
		super.init();
		this.cache = getCache(igniteCacheName,String.class,String.class);
	}
	public void init(String igniteConf){
		super.init(igniteConf);
		this.cache = getCache(igniteCacheName,String.class,String.class);
	}
	
	public void clear(){
		//this.cache.clear();
        for (Cache.Entry<String, String> entry : cache) {
        	cache.remove(entry.getKey());
        }
	}
	
	public void put(HashMap<String,String> map){
		cache.putAll(map);
		logger.info("list:"+map.size()+" rows saved");
	}
	
	public void destroy() {
		if(this.cache!=null)
			this.cache.close();
		super.destroy();
	}
	
}
