package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {
	private static final long serialVersionUID = 1L;
	
	private TransactionId transaction_id;
	private int table_id;
	private String table_alias;
	private DbFileIterator itr;

	/**
	 * Creates a sequential scan over the specified table as a part of the specified
	 * transaction.
	 *
	 * @param tid
	 *            The transaction this scan is running as a part of.
	 * @param tableid
	 *            the table to scan.
	 * @param tableAlias
	 *            the alias of this table (needed by the parser); the returned
	 *            tupleDesc should have fields with name tableAlias.fieldName (note:
	 *            this class is not responsible for handling a case where tableAlias
	 *            or fieldName are null. It shouldn't crash if they are, but the
	 *            resulting name can be null.fieldName, tableAlias.null, or
	 *            null.null).
	 */
	public SeqScan(TransactionId tid, int tableid, String tableAlias) {
		// some code goes here
		transaction_id = tid;
		table_id = tableid;
		table_alias = tableAlias;
		itr = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
	}

	/**
	 * @return return the table name of the table the operator scans. This should be
	 *         the actual name of the table in the catalog of the database
	 */
	public String getTableName() {
		return Database.getCatalog().getTableName(table_id);
	}

	/**
	 * @return Return the alias of the table this operator scans.
	 */
	public String getAlias() {
		// some code goes here
		return table_alias;
	}

	/**
	 * Reset the tableid, and tableAlias of this operator.
	 * 
	 * @param tableid
	 *            the table to scan.
	 * @param tableAlias
	 *            the alias of this table (needed by the parser); the returned
	 *            tupleDesc should have fields with name tableAlias.fieldName (note:
	 *            this class is not responsible for handling a case where tableAlias
	 *            or fieldName are null. It shouldn't crash if they are, but the
	 *            resulting name can be null.fieldName, tableAlias.null, or
	 *            null.null).
	 */
	public void reset(int tableid, String tableAlias) {
		// some code goes here
		table_id = tableid;
		table_alias = tableAlias;
	}

	public SeqScan(TransactionId tid, int tableId) {
		this(tid, tableId, Database.getCatalog().getTableName(tableId));
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		itr.open();
	}

	/**
	 * Returns the TupleDesc with field names from the underlying HeapFile, prefixed
	 * with the tableAlias string from the constructor. This prefix becomes useful
	 * when joining tables containing a field(s) with the same name. The alias and
	 * name should be separated with a "." character (e.g., "alias.fieldName").
	 *
	 * @return the TupleDesc with field names from the underlying HeapFile, prefixed
	 *         with the tableAlias string from the constructor.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		TupleDesc td = Database.getCatalog().getDatabaseFile(table_id).getTupleDesc();
		Type[] typeAr = new Type[td.numFields()];
		String[] fieldAr = new String[td.numFields()];
		
		String alias = table_alias;
		if (alias == null) alias = "null";
		
		for (int i = 0; i < td.numFields(); i++) {
			typeAr[i] = td.getFieldType(i);
			
			String field_name = td.getFieldName(i);
			if (field_name == null) field_name = "null";
			fieldAr[i] = alias + "." + field_name;
		}
		
		return new TupleDesc(typeAr, fieldAr);
	}

	public boolean hasNext() throws TransactionAbortedException, DbException {
		// some code goes here
		return itr.hasNext();
	}

	public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
		// some code goes here
		return itr.next();
	}

	public void close() {
		// some code goes here
		itr.close();
	}

	public void rewind() throws NoSuchElementException, TransactionAbortedException, DbException {
		// some code goes here
		itr.rewind();
	}
}
