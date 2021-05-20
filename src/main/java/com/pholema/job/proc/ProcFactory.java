package com.pholema.job.proc;

import org.apache.log4j.Logger;

public class ProcFactory {
	final static Logger logger = Logger.getLogger(ProcFactory.class);

	public static ProcImpl build(String procPackage) {
		// String procPackage = "com.pholema.job.proc.ignite.dynamic.string";
		try {
			String procClassPath = procPackage.concat(".Proc");
			logger.info("procClassPath : " + procClassPath);
			Class c = Class.forName(procClassPath);
			ProcImpl impl = (ProcImpl) c.newInstance();
			return impl;
		} catch (Exception e) {
			logger.error(e.toString());
			e.printStackTrace();
		}
		return null;
	}
}
