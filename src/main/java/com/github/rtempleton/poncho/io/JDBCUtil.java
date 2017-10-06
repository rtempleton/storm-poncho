package com.github.rtempleton.poncho.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rtempleton.poncho.io.parsers.BigDecimalParser;
import com.github.rtempleton.poncho.io.parsers.BooleanParser;
import com.github.rtempleton.poncho.io.parsers.DateParser;
import com.github.rtempleton.poncho.io.parsers.DoubleParser;
import com.github.rtempleton.poncho.io.parsers.FloatParser;
import com.github.rtempleton.poncho.io.parsers.IntParser;
import com.github.rtempleton.poncho.io.parsers.LongParser;
import com.github.rtempleton.poncho.io.parsers.ShortParser;
import com.github.rtempleton.poncho.io.parsers.StringParser;
import com.github.rtempleton.poncho.io.parsers.TimeParser;
import com.github.rtempleton.poncho.io.parsers.TimestampParser;
import com.github.rtempleton.poncho.io.parsers.TokenParser;

public class JDBCUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(JDBCUtil.class);

	/**
	 * 
	 * @param type - String representation of the database column type
	 * @param colName - the name of the database column for parsing logging purposes
	 * @param nullVal - default value for null values or parsing errors
	 * @param formatMask - Decimal/SimpleDateFormat mask for applicable parsers (see each parser type for default values)
	 * @return Data typed token parser for the g
	 */
	
	public static TokenParser getJDBCTokenParser(String jdbcType, String colName) {
		return getJDBCTokenParser(jdbcType, colName, java.sql.Types.NULL, null);
	}
	
	public static TokenParser getJDBCTokenParser(String jdbcType, String colName, Object nullVal, String formatMask) {
		
		switch (jdbcType.toUpperCase()) {
		
		case "VARCHAR":
		case "CHAR":
			return new StringParser(colName, nullVal);
			
		case "INTEGER":
		case "UNSIGNED_INT":
			return new IntParser(colName, formatMask, nullVal);
			
		case "SMALLINT":
		case "UNSIGNED_SMALLINT":
			return new ShortParser(colName, formatMask, nullVal);
			
		case "FLOAT":
		case "UNSIGNED_FLOAT":
			return new FloatParser(colName, formatMask, nullVal);
			
		case "DOUBLE":
		case "UNSIGNED_DOUBLE":
			return new DoubleParser(colName, formatMask, nullVal);
			
		case "BIGINT":
		case "UNSIGNED_LONG":
			return new LongParser(colName, formatMask, nullVal);
			
		case "DECIMAL":
			return new BigDecimalParser(colName, nullVal);
			
		case "BOOLEAN":
			return new BooleanParser(colName, null);
			
		case "TIME":
			return new TimeParser(colName, formatMask, nullVal);
			
		case "DATE":
			return new DateParser(colName, formatMask, nullVal);
			
		case "TIMESTAMP":
			return new TimestampParser(colName, formatMask, nullVal);

		default:
			logger.error("Unable to determine token parser for jdbcType: " + jdbcType);
			return new StringParser(colName, nullVal);
		}
	
	}
	
	public static int getSqlType(String jdbcType) {
		
		switch (jdbcType.toUpperCase()) {
		
		case "VARCHAR":
			return java.sql.Types.VARCHAR;
			
		case "CHAR":
			return java.sql.Types.CHAR;
			
		case "INTEGER":
			return java.sql.Types.INTEGER;
			
		case "UNSIGNED_INT":
			return java.sql.Types.INTEGER;
			
		case "SMALLINT":
			return java.sql.Types.SMALLINT;
			
		case "UNSIGNED_SMALLINT":
			return java.sql.Types.SMALLINT;
			
		case "FLOAT":
			return java.sql.Types.FLOAT;
			
		case "UNSIGNED_FLOAT":
			return java.sql.Types.FLOAT;
			
		case "DOUBLE":
			return java.sql.Types.DOUBLE;
			
		case "UNSIGNED_DOUBLE":
			return java.sql.Types.DOUBLE;
			
		case "BIGINT":
			return java.sql.Types.BIGINT;
			
		case "UNSIGNED_LONG":
			return java.sql.Types.BIGINT;
			
		case "DECIMAL":
			return java.sql.Types.DECIMAL;
			
		case "BOOLEAN":
			return java.sql.Types.BOOLEAN;
			
		case "TIME":
			return java.sql.Types.TIME;
			
		case "DATE":
			return java.sql.Types.DATE;
			
		case "TIMESTAMP":
			return java.sql.Types.TIMESTAMP;

		default:
			logger.error("Unable to determine SQL Type for jdbcType: " + jdbcType);
			return java.sql.Types.OTHER;
		}
		
	
	}

}
