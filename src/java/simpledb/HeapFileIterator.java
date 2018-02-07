package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
	private HeapFile file;
    private TransactionId tid;
    private boolean is_open = false;
    private int page_number = 0;
	private Iterator<Tuple> itr;
    
    public HeapFileIterator(HeapFile file, TransactionId tid) {
    		this.file = file;
    		this.tid = tid;
    }
    
	public void open() throws DbException, TransactionAbortedException {
		is_open = true;
		page_number = 0;
		itr = ((HeapPage) (Database.getBufferPool().getPage(tid, new HeapPageId(file.getId(), page_number), Permissions.READ_ONLY))).iterator();
	}

	public boolean hasNext() throws DbException, TransactionAbortedException {
		if (!is_open) return false;
		
		while (!itr.hasNext()) {
			if (page_number == file.numPages() - 1) return false;
			
			page_number++;
			itr = ((HeapPage) (Database.getBufferPool().getPage(tid, new HeapPageId(file.getId(), page_number), Permissions.READ_ONLY))).iterator();
		}
		
		return true;
	}

	public Tuple next() throws TransactionAbortedException, NoSuchElementException {
		if (!is_open) throw new NoSuchElementException("File not open.");
		if (itr.hasNext()) return itr.next();
		
		return null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		if (!is_open) throw new DbException("File not open.");
		
		page_number = 0;
		itr = ((HeapPage) (Database.getBufferPool().getPage(tid, new HeapPageId(file.getId(), page_number), Permissions.READ_ONLY))).iterator();
	}

	public void close() {
		is_open = false;
	}
}
