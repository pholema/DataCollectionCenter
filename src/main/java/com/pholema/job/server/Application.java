package com.pholema.job.server;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pholema.job.proc.ProcFactory;
import com.pholema.job.proc.ProcImpl;
import com.pholema.tool.utils.dynamic.ParameterException;
import com.pholema.tool.utils.dynamic.PropertiesUtils;

public class Application {
	static {
		init();
	}
	final static Logger logger = Logger.getLogger(Application.class);

	public static void init() {
		try {
			PropertiesUtils.init();
		} catch (ParameterException e) {
			e.printStackTrace();
		}

		String log4j_configuration = PropertiesUtils.properties.getProperty("log4j.configuration");
		System.setProperty("custom.log4j.output", PropertiesUtils.properties.getProperty("custom.log4j.output"));
		System.out.println("log4j_configuration:" + log4j_configuration);
		PropertyConfigurator.configure(log4j_configuration); // log4j init
	}

	public static void main(String[] args) throws Exception {
		try {
			String procPackage = PropertiesUtils.properties.getProperty("proc.package");
			logger.info("procPackage:" + procPackage);

			ProcImpl proc = ProcFactory.build(procPackage);
			proc.init();
			proc.run();
			proc.quit();

		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			logger.error(sw.toString());
		} finally {
			logger.info("job end.");
			System.exit(0);
		}
	}
}
