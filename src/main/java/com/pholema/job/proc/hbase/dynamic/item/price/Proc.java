package com.pholema.job.proc.hbase.dynamic.item.price;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.pholema.tool.utils.common.DateUtils;
import com.pholema.tool.utils.dynamic.PropertiesUtils;

public class Proc extends com.pholema.job.proc.hbase.dynamic.Proc {
	final static Logger logger = Logger.getLogger(Proc.class);
	private static Integer startOffset = Integer.valueOf(PropertiesUtils.properties.getProperty("db.sql.start.offset"));
	private static Integer endOffset = Integer.valueOf(PropertiesUtils.properties.getProperty("db.sql.end.offset"));

	@Override
	protected void setSQLStatement(PreparedStatement stmt) throws SQLException {
		java.sql.Timestamp timestampStart = new java.sql.Timestamp(DateUtils.getDateStart(startOffset).getTime());
		java.sql.Timestamp timestampEnd = new java.sql.Timestamp(DateUtils.getDateStart(endOffset).getTime());
		stmt.setTimestamp(1, timestampStart);
		stmt.setTimestamp(2, timestampEnd);
		stmt.setTimestamp(3, timestampStart);
		stmt.setTimestamp(4, timestampEnd);
	}

}
