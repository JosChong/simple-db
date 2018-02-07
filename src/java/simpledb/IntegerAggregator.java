package simpledb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int group_by_field;
    private Type group_by_field_type;
    private int agg_field;
    private Op agg_operator;
    private ConcurrentHashMap<Field, Integer> aggregates;
    private ConcurrentHashMap<Field, Integer> counts;
    private Field no_group_by_key;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    		group_by_field = gbfield;
        group_by_field_type = gbfieldtype;
        agg_field = afield;
        agg_operator= what;
        aggregates = new ConcurrentHashMap<Field, Integer>();
        counts = new ConcurrentHashMap<Field, Integer>();
        
        no_group_by_key = new IntField(0);
        if (group_by_field == Aggregator.NO_GROUPING) {
        		aggregates.put(no_group_by_key, 0);
        		counts.put(no_group_by_key, 0);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
	    	Field key;
	    	if (group_by_field == Aggregator.NO_GROUPING) {
	    		key = no_group_by_key;
	    	} else {
	    		key = tup.getField(group_by_field);
	    	}
	    	
	    	int new_value = ((IntField) (tup.getField(agg_field))).getValue();
	    	
	    	if (group_by_field == Aggregator.NO_GROUPING || tup.getTupleDesc().getFieldType(group_by_field).equals(group_by_field_type)) {
	    		if (aggregates.containsKey(key)) {
	    			aggregates.put(key, aggregate(aggregates.get(key), new_value));
			    	counts.put(key, counts.get(key) + 1);
	    		} else {
	    			aggregates.put(key, new_value);
	    			counts.put(key, 1);
	    		}
	    	}
    }
    
    private int aggregate(int agg, int new_value) {
        switch (agg_operator) {
        		case COUNT:
        			return agg + 1;
        		case SUM:
        			return agg + new_value;
        		case AVG:
        			return agg + new_value;
	        case MIN:
	            return Math.min(agg, new_value);
	        case MAX:
	            return Math.max(agg, new_value);
	        default:
	        		return 0;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
	    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
	    	TupleDesc td;
	    	
	    	if (group_by_field == Aggregator.NO_GROUPING) {
	    		td = new TupleDesc(new Type[] { Type.INT_TYPE });
	    		Tuple tuple = new Tuple(td);
	    		
	    		int aggregateVal;
	    		if (agg_operator == Aggregator.Op.COUNT) {
	    			aggregateVal = counts.get(no_group_by_key);
	    		} else {
	    			aggregateVal = aggregates.get(no_group_by_key);
	    			if (agg_operator == Aggregator.Op.AVG) {
	    				aggregateVal /= counts.get(no_group_by_key);
	    			}
	    		}
	    		
	    		tuple.setField(0, new IntField(aggregateVal));
	    		tuples.add(tuple);
	    	} else {
	    		td = new TupleDesc(new Type[] { group_by_field_type, Type.INT_TYPE });
		    	
		    	for (Field groupVal : aggregates.keySet())
		    	{
		    		Tuple tuple = new Tuple(td);
		    		
		    		int aggregateVal;
		    		if (agg_operator == Aggregator.Op.COUNT) {
		    			aggregateVal = counts.get(groupVal);
		    		} else {
		    			aggregateVal = aggregates.get(groupVal);
		    			if (agg_operator == Aggregator.Op.AVG) {
		    				aggregateVal /= counts.get(groupVal);
		    			}
		    		}
		    		
		    		tuple.setField(0, groupVal);
		    		tuple.setField(1, new IntField(aggregateVal));
		    		tuples.add(tuple);
		    	}
	    	}
	    	
	    return new TupleIterator(td, tuples);
    }
}
