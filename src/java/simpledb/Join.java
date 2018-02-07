package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    
    private JoinPredicate join_predicate;
    private OpIterator child_left;
    private OpIterator child_right;
    private Tuple current_left_tuple;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        join_predicate = p;
        child_left = child1;
        child_right = child2;
        current_left_tuple = null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return join_predicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        String JoinFieldName_child1 = child_left.getTupleDesc().getFieldName(join_predicate.getField1());
        return JoinFieldName_child1;
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        String JoinFieldName_child2 = child_right.getTupleDesc().getFieldName(join_predicate.getField2());
        return JoinFieldName_child2;
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child_left.getTupleDesc(), child_right.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child_left.open();
        child_right.open();
        super.open();
    }

    public void close() {
        // some code goes here
    		super.close();
        child_right.close();
        child_left.close();
    }

    // TODO set some sort of 'current left tuple' variable?
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child_left.rewind();
        child_right.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while ((child_left.hasNext()) || (current_left_tuple != null)) {
            Tuple left = null;

            if (current_left_tuple != null) {
                left = current_left_tuple;
            } else {
                current_left_tuple = child_left.next();
                left = current_left_tuple;
            }

            while (child_right.hasNext()) {
                Tuple right = child_right.next();
                Boolean tuple_join_check = join_predicate.filter(left, right);

                if (tuple_join_check) {
                    Tuple merged_tuple = new Tuple(getTupleDesc());
                    int left_size = left.getTupleDesc().numFields();
                    int right_size = right.getTupleDesc().numFields();

                    for (int i = 0; i < left_size; i++) {
                        merged_tuple.setField(i, left.getField(i));
                    }

                    for(int i = 0; i < right_size; i++){
                        merged_tuple.setField(left_size + i, right.getField(i));
                    }
                    
                    return merged_tuple;
                }
            }
            
            current_left_tuple = null;
            child_right.rewind();
        }
        
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
    		return new OpIterator[] { child_left, child_right };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    		child_left = children[0];
    		child_right = children[1];
    }

}