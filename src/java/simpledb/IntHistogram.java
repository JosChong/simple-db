package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private int[] buckets;
	private int bucketRange;
	private int minValue;
	private int maxValue;
	private int tupleCount;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    		this.buckets = new int[buckets];
    		minValue = min;
    		maxValue = max;
    		
    		// Calculate and set bucket width
    		bucketRange = (int) Math.ceil((max - min + 1.0) / buckets);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    		int bucket = (v - minValue) / bucketRange;
    		buckets[bucket]++;
    		tupleCount++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
    	
    		// Variable initialization
    		int bucket = (v - minValue) / bucketRange;
        int bucketStart = bucket * bucketRange + minValue;
        int bucketEnd = bucket * bucketRange + minValue + bucketRange - 1;
        
        // Switch statement for operator argument
        switch (op) {
        		case EQUALS:
        			if (v < minValue || v > maxValue) {
        				return 0;
        			} else {
        				return (double) buckets[bucket] / bucketRange / tupleCount;
        			}
        		case NOT_EQUALS:
        			if (v < minValue || v > maxValue) {
        				return 1;
        			} else {
        				return 1 - (double) buckets[bucket] / bucketRange / tupleCount;
        			}
        		case LESS_THAN:
        			if (v <= minValue) {
        				return 0;
        			} else if (v > maxValue) {
        				return 1;
        			} else {
        				double selectivity = (double) buckets[bucket] / tupleCount * (v - bucketStart) / bucketRange;
            			for (int i = bucket - 1; i >= 0; i--) {
                			selectivity += (double) buckets[i] / tupleCount;
                		}
            			return selectivity;
        			}
        		case LESS_THAN_OR_EQ:
        			if (v < minValue) {
        				return 0;
        			} else if (v >= maxValue) {
        				return 1;
        			} else {
        				double selectivity = (double) buckets[bucket] / tupleCount * (v - bucketStart) / bucketRange + (double) buckets[bucket] / bucketRange / tupleCount;
        				for (int i = bucket - 1; i >= 0; i--) {
                			selectivity += (double) buckets[i] / tupleCount;
                		}
            			return selectivity;
        			}
        		case GREATER_THAN:
        			if (v < minValue) {
        				return 1;
        			} else if (v >= maxValue) {
        				return 0;
        			} else {
        				double selectivity = (double) buckets[bucket] / tupleCount * (bucketEnd - v) / bucketRange;
            			for (int i = bucket + 1; i < buckets.length; i++) {
                			selectivity += (double) buckets[i] / tupleCount;
                		}
            			return selectivity;
        			}
        		case GREATER_THAN_OR_EQ:
        			if (v <= minValue) {
        				return 1;
        			} else if (v > maxValue) {
        				return 0;
        			} else {
        				double selectivity = (double) buckets[bucket] / tupleCount * (bucketEnd - v) / bucketRange + (double) buckets[bucket] / bucketRange / tupleCount;
            			for (int i = bucket + 1; i < buckets.length; i++) {
                			selectivity += (double) buckets[i] / tupleCount;
                		}
            			return selectivity;
        			}
        		default:
        			return -1;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
