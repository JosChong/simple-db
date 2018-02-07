package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    
    private OpIterator child_iterator;
    private int agg_field;
    private int group_by_field;
    private Aggregator.Op agg_operator;
    private OpIterator itr;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
    		// some code goes here
    		child_iterator = child;
    		agg_field = afield;
    		group_by_field = gfield;
    		agg_operator = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    		// some code goes here
    		if (group_by_field == -1) return Aggregator.NO_GROUPING;
    		return group_by_field;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
    		// some code goes here
    		if (group_by_field == -1) return null;
		return child_iterator.getTupleDesc().getFieldName(group_by_field);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    		// some code goes here
    		return agg_field;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    		// some code goes here
    		return child_iterator.getTupleDesc().getFieldName(agg_field);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    		// some code goes here
    		return agg_operator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    		return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    		// some code goes here
	    	Type group_by_field_type;
	    	if (group_by_field == Aggregator.NO_GROUPING) {
	    		group_by_field_type = null;
	    	} else {
	    		group_by_field_type = child_iterator.getTupleDesc().getFieldType(group_by_field);
	    	}
	    	
	    	Aggregator agg;
	    	if (child_iterator.getTupleDesc().getFieldType(agg_field).equals(Type.INT_TYPE)) {
	    		agg = new IntegerAggregator(group_by_field, group_by_field_type, agg_field, agg_operator);
	    	} else {
	    		agg = new StringAggregator(group_by_field, group_by_field_type, agg_field, agg_operator);
	    	}
	    	
	    	child_iterator.open();
	    	while (child_iterator.hasNext()) agg.mergeTupleIntoGroup(child_iterator.next());
	    	child_iterator.close();
	    	
	    	itr = agg.iterator();
	    	itr.open();
	    	super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    		// some code goes here
    		if (itr.hasNext()) {
    			return itr.next();
    		} else {
    			return null;
    		}
    }

    public void rewind() throws DbException, TransactionAbortedException {
    		// some code goes here
    		itr.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    		// some code goes here
	    	String agg_name = agg_operator.toString() + child_iterator.getTupleDesc().getFieldName(agg_field);

        	if (group_by_field == Aggregator.NO_GROUPING) {
        		return new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { agg_name });
        	} else {
        		return new TupleDesc(new Type[] { child_iterator.getTupleDesc().getFieldType(group_by_field), Type.INT_TYPE },
        				new String[] { child_iterator.getTupleDesc().getFieldName(group_by_field), agg_name });
        	}
    }

    public void close() {
    		// some code goes here
    		super.close();
    		itr.close();
    }

    @Override
    public OpIterator[] getChildren() {
    		// some code goes here
    		return new OpIterator[] { child_iterator };
    }

    @Override
    public void setChildren(OpIterator[] children) {
    		// some code goes here
	    	child_iterator = children[0];
    }
    
}
