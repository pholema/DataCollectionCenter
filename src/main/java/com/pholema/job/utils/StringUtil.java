package com.pholema.job.utils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.google.gson.JsonObject;

public class StringUtil {
	private static String SQLType2JavaTypeInString(String source_type) {
		String java_type = null;
		if (source_type.startsWith("money") || source_type.startsWith("decimal") || source_type.startsWith("smallmoney")) {
			java_type = "double";
		} else if (source_type.startsWith("numeric")) {
			java_type = "bigdecimal";
		} else if (source_type.startsWith("int") || source_type.startsWith("smallint") || source_type.startsWith("tinyint") || source_type.startsWith("bit")) {
			java_type = "integer";
		} else if (source_type.startsWith("bigint")) {
			java_type = "long";
		} else if (source_type.startsWith("real") || source_type.startsWith("float")) {
			java_type = "float";
		} else if (source_type.startsWith("varchar") || source_type.startsWith("longvarchar") || source_type.startsWith("character") || source_type.startsWith("nvarchar") || source_type.startsWith("char") || source_type.startsWith("nchar") || source_type.startsWith("text") || source_type.startsWith("ntext") || source_type.startsWith("uniqueidentifier") || source_type.startsWith("sysname")) {
			java_type = "string";
		} else if (source_type.startsWith("date") || source_type.startsWith("datetime") || source_type.startsWith("datetime2") || source_type.startsWith("time") || source_type.startsWith("smalldatetime")) {
			java_type = "date";
		}
		return java_type;
	}

	public static JsonObject sqlResultToJsonObject(ResultSet res, ResultSetMetaData resultSetMetaData) throws SQLException {
		JsonObject object = new JsonObject();
		for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
			String columnName = resultSetMetaData.getColumnName(i);

			if (res.getString(columnName) != null) {
				String java_type = StringUtil.SQLType2JavaTypeInString(resultSetMetaData.getColumnTypeName(i));
				if (java_type.equals("double")) {
					object.addProperty(columnName, res.getDouble(columnName));
				} else if (java_type.equals("bigdecimal")) {
					object.addProperty(columnName, res.getBigDecimal(columnName));
				} else if (java_type.equals("integer")) {
					object.addProperty(columnName, res.getInt(columnName));
				} else if (java_type.equals("long")) {
					object.addProperty(columnName, res.getLong(columnName));
				} else if (java_type.equals("float")) {
					object.addProperty(columnName, res.getFloat(columnName));
				} else if (java_type.equals("string")) {
					object.addProperty(columnName, res.getString(columnName).trim());
				} else if (java_type.equals("date")) {
					object.addProperty(columnName, res.getTimestamp(columnName).getTime());
				} else {
					object.addProperty(columnName, res.getString(columnName).trim());
				}
			}

		}
		return object;
	}

}
