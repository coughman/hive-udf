package com.cloudera.hive.udf.examples;

import java.util.HashSet;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class Union extends AbstractGenericUDAFResolver {
	static Logger logger = Logger.getLogger(Union.class);

	  @Override
	  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
	      throws SemanticException {
	    if (parameters.length < 1) {
	      throw new UDFArgumentTypeException(parameters.length - 1,
	          "At least one argument is expected.");
	    }

	    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
	      throw new UDFArgumentTypeException(0,
	          "Only primitive type arguments are accepted but "
	              + parameters[0].getTypeName() + " is passed.");
	    }
	    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
	    case STRING:
		      return new GenericUDAFUnion();	  	  
	    default:
	      throw new UDFArgumentTypeException(0,
	          "Only string arguments are accepted but "
	              + parameters[0].getTypeName() + " is passed.");
	    }
	  }	
	  
	  public static class GenericUDAFUnion extends GenericUDAFEvaluator {
		    private PrimitiveObjectInspector inputOI;
		    private Text result;
		    
		    @Override
		    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
		      assert (parameters.length >= 1);
		      super.init(m, parameters);
		      result = new Text("");
		      inputOI = (PrimitiveObjectInspector) parameters[0];
		   
		      return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		    }

		    /** class for storing value. */
		    @AggregationType(estimable = false)
		    static class UnionAgg extends AbstractAggregationBuffer {
		      boolean empty;
		      HashSet<String> result;
		    }

		    @Override
		    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
		      UnionAgg result = new UnionAgg();
		      reset(result);
		      return result;
		    }

		    @Override
		    public void reset(AggregationBuffer agg) throws HiveException {
		      UnionAgg myagg = (UnionAgg) agg;
		      myagg.empty = true;
		      myagg.result = new HashSet<String>();
		    }

		    //private boolean warned = false;

		    @Override
		    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
		      assert (parameters.length >= 1);
		      merge(agg, parameters[0]);
		    }

		    @Override
		    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
		      return terminate(agg);
		    }

		    @Override
		    public void merge(AggregationBuffer agg, Object partial) throws HiveException {	    	
		      if (partial != null) {
		        UnionAgg myagg = (UnionAgg) agg;
		        myagg.result.add(PrimitiveObjectInspectorUtils.getString(partial, inputOI));
		        myagg.empty = false;
		      }
		    }

		    @Override
		    public Object terminate(AggregationBuffer agg) throws HiveException {
		      UnionAgg myagg = (UnionAgg) agg;
		      if (myagg.empty) {
		        return null;
		      }
		      result.set(myagg.result.toString());
		      return result;
		    }

		  }

}
