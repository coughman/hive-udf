package com.cloudera.hive.udf.examples;

import java.math.BigInteger;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class Multiply extends AbstractGenericUDAFResolver {

	static Logger logger = Logger.getLogger(Multiply.class);
	
	  @Override
	  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
	      throws SemanticException {
	    if (parameters.length != 1) {
	      throw new UDFArgumentTypeException(parameters.length - 1,
	          "Exactly one argument is expected.");
	    }

	    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
	      throw new UDFArgumentTypeException(0,
	          "Only primitive type arguments are accepted but "
	              + parameters[0].getTypeName() + " is passed.");
	    }
	    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
	    case BYTE:
	    case SHORT:
	    case INT:
	    case LONG:
	    case STRING:
	      return new GenericUDAFMultiplyLong();	  
	    default:
	      throw new UDFArgumentTypeException(0,
	          "Only numeric arguments are accepted but "
	              + parameters[0].getTypeName() + " is passed.");
	    }
	  }	

	  /**
	   * GenericUDAFSumLong.
	   *
	   */
	  public static class GenericUDAFMultiplyLong extends GenericUDAFEvaluator {
	    private PrimitiveObjectInspector inputOI;
	    private Text result;

	    @Override
	    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
	      assert (parameters.length == 1);
	      super.init(m, parameters);
	      result = new Text("0");
	      inputOI = (PrimitiveObjectInspector) parameters[0];
	      return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	    }

	    /** class for storing double value. */
	    @AggregationType(estimable = true)
	    static class MultiplyLongAgg extends AbstractAggregationBuffer {
	      boolean empty;
	      BigInteger result;
	      @Override
	      public int estimate() { return JavaDataModel.PRIMITIVES2; }
	    }

	    @Override
	    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
	      MultiplyLongAgg result = new MultiplyLongAgg();
	      reset(result);
	      return result;
	    }

	    @Override
	    public void reset(AggregationBuffer agg) throws HiveException {
	      MultiplyLongAgg myagg = (MultiplyLongAgg) agg;
	      myagg.empty = true;
	      myagg.result = new BigInteger("0");
	    }

	    private boolean warned = false;

	    @Override
	    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
	      assert (parameters.length == 1);
	      try {
	        merge(agg, parameters[0]);
	      } catch (NumberFormatException e) {
	        if (!warned) {
	          warned = true;
	          logger.warn(getClass().getSimpleName() + " "
	              + StringUtils.stringifyException(e));
	        }
	      }
	    }

	    @Override
	    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
	      return terminate(agg);
	    }

	    @Override
	    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
	      if (partial != null) {
	    	  String value = PrimitiveObjectInspectorUtils.getString(partial, inputOI);
	    	  logger.debug("value: " + value);

	    	  if (value == null || value.equals("0")) {
	    		logger.warn("encountered null or zero value, skipping");
	    		return;
	    	  }
	    	  
	        MultiplyLongAgg myagg = (MultiplyLongAgg) agg;
	        
	        if (myagg.empty)
	    	   myagg.result = new BigInteger(value);
	        else
	    	   myagg.result = myagg.result.multiply(new BigInteger(value));
	        
	        logger.debug("result: " + myagg.result);
	        myagg.empty = false;
	      }
	    }

	    @Override
	    public Object terminate(AggregationBuffer agg) throws HiveException {
	      MultiplyLongAgg myagg = (MultiplyLongAgg) agg;
	      if (myagg.empty) {
	        return null;
	      }
	      result.set(myagg.result.toString());
	      return result;
	    }

	  }
}
