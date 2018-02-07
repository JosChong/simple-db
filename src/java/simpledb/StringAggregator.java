package simpledb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int group_by_field;
    private Type group_by_field_type;
    private int agg_field;
    private Op agg_operator;
    private ConcurrentHashMap<Field, Integer> counts;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    		group_by_field = gbfield;
    		group_by_field_type = gbfieldtype;
    		agg_field = afield;
    		agg_operator = what;
    		counts = new ConcurrentHashMap<Field, Integer>();
    		
	    	if (agg_operator != Aggregator.Op.COUNT) throw new IllegalArgumentException("only supports COUNT");
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
	    	if (tup.getTupleDesc().getFieldType(group_by_field).equals(group_by_field_type)) {
	    		Field group_key;
	    		if (group_by_field == Aggregator.NO_GROUPING) {
	    			group_key = null;
	    		} else {
	    			group_key = tup.getField(group_by_field);
	    		}
	
	    		if (counts.containsKey(group_key)) {
	    			counts.put(group_key, counts.get(group_key) + 1);
	    		} else {
	    			counts.put(group_key, 1);
	    		}
	    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
	    	TupleDesc td = new TupleDesc(new Type[] { group_by_field_type, Type.INT_TYPE });
	    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
	    	
	    	for (Field groupVal : counts.keySet())
	    	{
	    		Tuple tuple = new Tuple(td);
	    		int aggregateVal = counts.get(groupVal);
	    		
	    		if (group_by_field == Aggregator.NO_GROUPING){
	    			tuple.setField(0, new IntField(aggregateVal));
	    		}
	    		else {
	    			tuple.setField(0, groupVal);
	        		tuple.setField(1, new IntField(aggregateVal));    
	    		}
	    		
	    		tuples.add(tuple);
	    	}
		
	    	return new TupleIterator(td, tuples);
    }
}
