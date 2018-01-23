package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {
	private static final long serialVersionUID = 1L;

	private TupleDesc schema;
	private Field[] fields;
	private RecordId rid;

	/**
	 * Create a new tuple with the specified schema (type).
	 *
	 * @param td
	 *            the schema of this tuple. It must be a valid TupleDesc instance
	 *            with at least one field.
	 */
	public Tuple(TupleDesc td) {
		// some code goes here
		if (!(td instanceof TupleDesc) || td.numFields() < 1) return;
		schema = td;
		fields = new Field[td.numFields()];
	}

	/**
	 * @return The TupleDesc representing the schema of this tuple.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return schema;
	}

	/**
	 * @return The RecordId representing the location of this tuple on disk. May be
	 *         null.
	 */
	public RecordId getRecordId() {
		// some code goes here
		return rid;
	}

	/**
	 * Set the RecordId information for this tuple.
	 *
	 * @param rid
	 *            the new RecordId for this tuple.
	 */
	public void setRecordId(RecordId rid) {
		// some code goes here
		this.rid = rid;
	}

	/**
	 * Change the value of the ith field of this tuple.
	 *
	 * @param i
	 *            index of the field to change. It must be a valid index.
	 * @param f
	 *            new value for the field.
	 */
	public void setField(int i, Field f) throws NoSuchElementException {
		// some code goes here
		if (i < 0 || i >= fields.length) throw new NoSuchElementException("Invalid index.");
		fields[i] = f;
	}

	/**
	 * @return the value of the ith field, or null if it has not been set.
	 *
	 * @param i
	 *            field index to return. Must be a valid index.
	 */
	public Field getField(int i) throws NoSuchElementException {
		// some code goes here
		if (i < 0 || i >= fields.length) throw new NoSuchElementException("Invalid index.");
		return fields[i];
	}

	/**
	 * Returns the contents of this Tuple as a string. Note that to pass the system
	 * tests, the format needs to be as follows:
	 *
	 * column1\tcolumn2\tcolumn3\t...\tcolumnN
	 *
	 * where \t is any whitespace (except a newline)
	 */
	public String toString() {
		// some code goes here
		String s = fields[0].toString();
		if (fields.length > 1) {
			for (int i = 1; i < fields.length; i++) {
				s += "\t" + fields[i].toString();
			}
		}
		s += "\n";
		return s;
	}

	/**
	 * @return An iterator which iterates over all the fields of this tuple
	 */
	public Iterator<Field> fields() {
		// some code goes here
		return Arrays.asList(fields).iterator();
	}

	/**
	 * reset the TupleDesc of this tuple (only affecting the TupleDesc)
	 */
	public void resetTupleDesc(TupleDesc td) throws DbException {
		// some code goes here
		if (!(td instanceof TupleDesc) || td.numFields() < 1) throw new DbException("Invalid schema.");
		schema = td;
		fields = new Field[td.numFields()];
	}
}
