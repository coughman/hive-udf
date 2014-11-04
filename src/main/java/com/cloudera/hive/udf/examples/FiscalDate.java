package com.cloudera.hive.udf.examples;

import java.util.Calendar;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class FiscalDate extends UDF {
	static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	
	public Text evaluate(final Text dateString) {
		if (dateString == null) return null;
		Date date = null;
		try {
			date = DATE_FORMAT.parse(dateString.toString());
			Calendar c = Calendar.getInstance();
			c.setTime(date);
			c.add(Calendar.MONTH, -3);
			c.set(Calendar.DAY_OF_MONTH, 1);
			return new Text(DATE_FORMAT.format(c.getTime()));
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		} 
	}
}
