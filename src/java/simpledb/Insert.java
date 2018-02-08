package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private OpIterator child_iterator;
    private int table_id;
    private TupleDesc td;
    
    private boolean inserted;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    		tid = t;
    		child_iterator = child;
    		table_id = tableId;
    		
    		if (!Database.getCatalog().getTupleDesc(table_id).equals(child.getTupleDesc())) {
              	throw new DbException("TupleDesc of insert table and child are different");
         }
    		
    		Type[] type_array = new Type[] { Type.INT_TYPE };
    		String[] string_array = new String[] { "Number Inserted" };
        td = new TupleDesc(type_array, string_array);
            
        inserted = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    		child_iterator.open();
    		super.open();
    }

    public void close() {
        // some code goes here
    		super.close();
    		child_iterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    		child_iterator.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    		if (inserted) return null;
	    	
	    	int num_inserted = 0;
	    	while (child_iterator.hasNext())
	    	{
	    		try {
	        		Database.getBufferPool().insertTuple(tid, table_id, child_iterator.next());    			
	    		} catch (IOException e) {
	    			System.out.println(e.getMessage());
	    		}
	    		num_inserted++;
	    	}
	    	
	    	inserted = true;
	    	Tuple tuple = new Tuple(td);
	    	tuple.setField(0, new IntField(num_inserted));
	    	return tuple;
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
