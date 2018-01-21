package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {
	int tid;
	int page_num;

	/**
	 * Constructor. Create a page id structure for a specific page of a specific
	 * table.
	 *
	 * @param tableId
	 *            The table that is being referenced
	 * @param pgNo
	 *            The page number in that table.
	 */
	public HeapPageId(int tableId, int pgNo) {
		// some code goes here
		tid = tableId;
		page_num = pgNo;
	}

	/** @return the table associated with this PageId */
	public int getTableId() {
		// some code goes here
		return tid;
	}

	/**
	 * @return the page number in the table getTableId() associated with this PageId
	 */
	public int getPageNumber() {
		// some code goes here
		return page_num;
	}

	/**
	 * @return a hash code for this page, represented by the concatenation of the
	 *         table number and the page number (needed if a PageId is used as a key
	 *         in a hash table in the BufferPool, for example.)
	 * @see BufferPool
	 */
	public int hashCode() {
		// some code goes here
		String hash_code = Integer.toString(tid) + Integer.toString(page_num);
		return Integer.parseInt(hash_code);
	}

	/**
	 * Compares one PageId to another.
	 *
	 * @param o
	 *            The object to compare against (must be a PageId)
	 * @return true if the objects are equal (e.g., page numbers and table ids are
	 *         the same)
	 */
	public boolean equals(Object o) {
		// some code goes here
		if (o == null || !(o instanceof PageId)) return false;
		
		PageId pid = (PageId) o;
		if (tid == pid.getTableId() && page_num == pid.getPageNumber()) {
			return true;
		}
		return false;
	}

	/**
	 * Return a representation of this object as an array of integers, for writing
	 * to disk. Size of returned array must contain number of integers that
	 * corresponds to number of args to one of the constructors.
	 */
	public int[] serialize() {
		int data[] = new int[2];

		data[0] = getTableId();
		data[1] = getPageNumber();

		return data;
	}
}
