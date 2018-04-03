package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;
    
    private ConcurrentHashMap<Integer, IntHistogram> intHistogramHashMap;
    private ConcurrentHashMap<Integer, StringHistogram> strHistogramHashMap;
    
    private DbFile dbFile;
    private int pageCost;
    private int numTuples;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	
    		// Variable initialization
    		intHistogramHashMap = new ConcurrentHashMap<Integer, IntHistogram>();
        strHistogramHashMap = new ConcurrentHashMap<Integer, StringHistogram>();
    	
    		dbFile = Database.getCatalog().getDatabaseFile(tableid);
        pageCost = ioCostPerPage;
        numTuples = 0;

        DbFileIterator dbFileItr = dbFile.iterator(new TransactionId());
        TupleDesc td = dbFile.getTupleDesc();
        int[] minArray = new int[td.numFields()];
        int[] maxArray = new int[td.numFields()];

        try {
        		dbFileItr.open();
        		
            while (dbFileItr.hasNext()) {
                Tuple tuple = dbFileItr.next();
                Iterator<Field> fieldItr = tuple.fields();

                // Filling min and max arrays
                for (int i = 0; i < td.numFields(); i++) {
                    Field field = fieldItr.next();
                    if (field.getType() == Type.INT_TYPE) {
                        int fieldValue = ((IntField) field).getValue();
                        if (fieldValue < minArray[i]) {
                            minArray[i] = fieldValue;
                        } else if (fieldValue > maxArray[i]) {
                            maxArray[i] = fieldValue;
                        }
                    }
                }
                numTuples++;
            }

            Iterator<TupleDesc.TDItem> tdItr = td.iterator();

            // Instantiate histograms
            for (int i = 0; i < td.numFields(); i++) {
                if (tdItr.next().fieldType == Type.INT_TYPE) {
                    IntHistogram intHist = new IntHistogram(Math.min(NUM_HIST_BINS, maxArray[i] - minArray[i] + 1), minArray[i], maxArray[i]);
                    intHistogramHashMap.put(i, intHist);
                } else {
                    strHistogramHashMap.put(i, new StringHistogram(NUM_HIST_BINS));
                }
            }

            dbFileItr.rewind();
            
            // Add field values to histograms
            while (dbFileItr.hasNext()) {
                Tuple tuple = dbFileItr.next();
                Iterator<Field> fieldItr = tuple.fields();
                
                for (int i = 0; i < td.numFields(); i++) {
                    Field field = fieldItr.next();
                    if (field.getType() == Type.INT_TYPE) {
                        intHistogramHashMap.get(i).addValue(((IntField) field).getValue());
                    } else {
                        strHistogramHashMap.get(i).addValue(((StringField) field).getValue());
                    }
                }
            }
            
            dbFileItr.close();
        } catch (DbException exception) {
            exception.printStackTrace();
        } catch (TransactionAbortedException exception) {
            exception.printStackTrace();
        } 
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return (double) numTuples * dbFile.getTupleDesc().getSize() / BufferPool.getPageSize() * pageCost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (selectivityFactor * numTuples);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (constant.getType() == Type.INT_TYPE) {
        		return intHistogramHashMap.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        }
        return strHistogramHashMap.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return numTuples;
    }

}
