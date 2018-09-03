package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class
                    .getDeclaredField("statsMap");
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

    private int ioCostPerPage;
    private HeapFile table;
    private TupleDesc td;
    private int numTuples;
    private HashMap<String, Integer[]> nameTolimits;
    private HashMap<String, Object> nameTohistogram;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     * @throws TransactionAbortedException
     * @throws DbException
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.

        this.ioCostPerPage = ioCostPerPage;
        this.table = (HeapFile) Database.getCatalog().getDbFile(tableid);
        this.td = table.getTupleDesc();
        this.nameTolimits = new HashMap<>();
        this.nameTohistogram = new HashMap<>();
        scanTable();
    }

    /**
     * 创建histogram
     */
    private void scanTable() {
        DbFileIterator iter = table.iterator(new Transaction().getId());
        try {
            iter.open();
            // 获取int数据的min、max值，用来创建histogram
            while (iter.hasNext()) {
                numTuples++;
                Tuple t = iter.next();
                for (int i = 0; i < td.numFields(); i++) {
                    Type type = td.getFieldType(i);
                    if (type == Type.INT_TYPE) {
                        String name = td.getFieldName(i);
                        Integer value = ((IntField) t.getField(i)).getValue();
                        if (nameTolimits.containsKey(name)) {
                            Integer[] arr = nameTolimits.get(name);
                            arr[0] = Math.min(arr[0], value);
                            arr[1] = Math.max(arr[1], value);
                        } else {
                            Integer[] min_max = new Integer[] { value, value };
                            nameTolimits.put(name, min_max);
                        }
                    }
                }
            }

            // 创建histogram
            iter.rewind();
            while (iter.hasNext()) {
                Tuple tuple = iter.next();
                for (int i = 0; i < td.numFields(); i++) {
                    String fieldName = td.getFieldName(i);
                    Type fieldType = td.getFieldType(i);

                    if (fieldType == Type.INT_TYPE) {
                        int value = ((IntField) tuple.getField(i)).getValue();
                        if (nameTohistogram.containsKey(fieldName)) {
                            IntHistogram histogram = (IntHistogram) nameTohistogram
                                    .get(fieldName);
                            histogram.addValue(value);
                        } else {
                            Integer[] values = nameTolimits.get(fieldName);
                            IntHistogram histogram = new IntHistogram(
                                    NUM_HIST_BINS, values[0], values[1]);
                            nameTohistogram.put(fieldName, histogram);
                        }
                    } else {
                        // String类型的列，有可能需要新建histogram
                        String value = ((StringField) tuple.getField(i))
                                .getValue();
                        if (nameTohistogram.containsKey(fieldName)) {
                            StringHistogram histogram = (StringHistogram) nameTohistogram
                                    .get(fieldName);
                            histogram.addValue(value);
                        } else {
                            StringHistogram stringHistogram = new StringHistogram(
                                    NUM_HIST_BINS);
                            stringHistogram.addValue(value);
                            nameTohistogram.put(fieldName, stringHistogram);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
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
        return this.ioCostPerPage * ((HeapFile) table).numPages();
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
        return (int) Math.ceil(numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * 
     * @param field
     *            the index of the field
     * @param op
     *            the operator in the predicate The semantic of the method is
     *            that, given the table, and then given a tuple, of which we do
     *            not know the value of the field, return the expected
     *            selectivity. You may estimate this value from the histograms.
     */
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
    public double estimateSelectivity(int index, Predicate.Op op,
            Field constant) {
        String name = td.getFieldName(index);
        if (constant.getType() == Type.INT_TYPE) {
            IntHistogram histogram = (IntHistogram) this.nameTohistogram.get(name);
            return histogram.estimateSelectivity(op,
                    ((IntField) constant).getValue());
        } else {
            StringHistogram histogram = (StringHistogram) this.nameTohistogram
                    .get(name);
            return histogram.estimateSelectivity(op,
                    ((StringField) constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return this.numTuples;
    }

}
