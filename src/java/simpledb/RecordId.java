package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private PageId page_id;
	private int tuple_number;

	/**
	 * Creates a new RecordId referring to the specified PageId and tuple number.
	 * 
	 * @param pid
	 *            the pageid of the page on which the tuple resides
	 * @param tupleno
	 *            the tuple number within the page.
	 */
	public RecordId(PageId pid, int tupleno) {
		// some code goes here
		page_id = pid;
		tuple_number = tupleno;
	}

	/**
	 * @return the tuple number this RecordId references.
	 */
	public int getTupleNumber() {
		// some code goes here
		return tuple_number;
	}

	/**
	 * @return the page id this RecordId references.
	 */
	public PageId getPageId() {
		// some code goes here
		return page_id;
	}

	/**
	 * Two RecordId objects are considered equal if they represent the same tuple.
	 * 
	 * @return True if this and o represent the same tuple
	 */
	@Override
	public boolean equals(Object o) {
		// some code goes here
		if (o == null || !(o instanceof RecordId)) return false;
		
		RecordId rid = (RecordId) o;
		if (page_id.equals(rid.getPageId()) && tuple_number == rid.getTupleNumber()) return true;
		return false;
	}

	/**
	 * You should implement the hashCode() so that two equal RecordId instances
	 * (with respect to equals()) have the same hashCode().
	 * 
	 * @return An int that is the same for equal RecordId objects.
	 */
	@Override
	public int hashCode() {
		// some code goes here
		String hash_code = Integer.toString(page_id.hashCode()) + Integer.toString(tuple_number);
		return hash_code.hashCode();
	}
}
