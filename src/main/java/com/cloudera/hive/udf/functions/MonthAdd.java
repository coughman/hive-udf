/**
 * 
 */
package com.cloudera.hive.udf.functions;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.log4j.Logger;

/**
 * MonthAdd
 *
 */
@Description(name = "month_add",
    value = "_FUNC_(date, num_months) - Returns the date that is num_months after date.",
    extended = "date is a string in the format 'yyyy-MM-dd HH:mm:ss' or"
    + " 'yyyy-MM-dd'. num_months is a number.\n")
public class MonthAdd extends GenericUDF {

	private ObjectInspector[] ois;
	private GenericUDFUtils.ReturnObjectInspectorResolver roir;
	static Logger logger = Logger.getLogger(MonthAdd.class);
	
	private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	private final Calendar calendar = Calendar.getInstance();	
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if (arguments.length != 2)
			throw new UDFArgumentException("date and month arguments are required");
	
		if (!arguments[1].getTypeName().equals("int"))
			throw new UDFArgumentException("second argument must be integer");
	
			
		this.ois = arguments;
		roir = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		roir.update(arguments[0]);
		return roir.get();
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		String dateString = String.valueOf(roir.convertIfNecessary(arguments[0].get(),ois[0]));
		Object incrementArg = arguments[1].get();
		
		if (dateString == null || dateString.equals("") ||
			incrementArg == null || String.valueOf(incrementArg).equals("null"))
			return null;
		
		int increment = Integer.valueOf(String.valueOf(arguments[1].get()));
		
		try {
			Date date = formatter.parse(dateString);
			calendar.setTime(date);
			calendar.add(Calendar.MONTH, increment);
			Date outDate = calendar.getTime();
			return new TimestampWritable(new Timestamp(outDate.getTime()));
		} catch (ParseException e) {
			logger.error("failed to parse date", e);
			return null;
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return "MonthAdd";
	}

}
