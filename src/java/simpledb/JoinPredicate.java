package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private int tuple1_field;
    private int tuple2_field;
    private Predicate.Op compare_operation;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        tuple1_field = field1;
        tuple2_field = field2;
        compare_operation = op;

    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        Boolean compare_result = t1.getField(tuple1_field).compare(compare_operation, t2.getField(tuple2_field));
        return compare_result;
    }
    
    public int getField1()
    {
        // some code goes here
        return tuple1_field;
    }
    
    public int getField2()
    {
        // some code goes here
        return tuple2_field;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return compare_operation;
    }
}
