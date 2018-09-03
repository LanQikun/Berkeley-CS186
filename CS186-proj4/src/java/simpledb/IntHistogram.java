package simpledb;

import simpledb.Predicate.Op;

// You may modify StringHistogram if you want to implement a better estimator

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 */

/**
 * 注意：由于Math.ceil的舍入，最后一个桶的最大值可能超过maxValue，所以要调整相应的width和最大值。
 */

public class IntHistogram {
    private int numBuckets;
    private double[] buckets;
    // 每个桶的最大值
    private double[] compareArr;
    private int minValue;
    private int maxValue;
    private int width;
    // 最后一个桶的width
    private int lastWidth;
    private int count;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it
     * receives. It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time
     * through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are
     * both constant with respect to the number of values being histogrammed.
     * For example, you shouldn't simply store every value that you see in a
     * sorted list.
     * 
     * @param buckets
     *            The number of buckets to split the input value into.
     * @param min
     *            The minimum integer value that will ever be passed to this
     *            class for histogramming
     * @param max
     *            The maximum integer value that will ever be passed to this
     *            class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.numBuckets = buckets;
        this.buckets = new double[numBuckets];
        this.minValue = min;
        this.maxValue = max;
        this.width = (int) Math.ceil((max - min + 1) / (double) numBuckets);

        this.compareArr = new double[numBuckets];
        // 获取每个桶的最大值
        for (int i = 0; i < numBuckets - 1; i++) {
            compareArr[i] = minValue - 1 + width * (i + 1);
        }
        // 由于Math.ceil的舍入，最后一个桶的最大值可能超过maxValue
        if (numBuckets > 0) {
            int lastMin = minValue + width * (numBuckets - 1);
            this.lastWidth = maxValue - lastMin + 1;
            compareArr[numBuckets - 1] = lastMin + lastWidth - 1;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * 
     * @param val
     *            Value to add to the histogram
     */
    public void addValue(int val) {
        buckets[getIndex(val)]++;
        count++;
    }

    private int getIndex(int val) {
        return (val - minValue) / width;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this
     * table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, return your estimate
     * of the fraction of elements that are greater than 5.
     * 
     * @param op
     *            Operator
     * @param val
     *            Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int val) {
        int index = getIndex(val);
        int w = index == numBuckets - 1 ? lastWidth : width;
        if (op == Op.EQUALS) {
            if (val >= minValue && val <= maxValue) {
                return buckets[index] / w / count;
            } else {
                return 0;
            }
        } else if (op == Op.GREATER_THAN) {
            if (val >= minValue && val <= maxValue) {
                double greater = buckets[index] * ((compareArr[index] - val) / w);
                for (int i = index + 1; i < numBuckets; i++) {
                    greater += buckets[i];
                }
                return greater / count;
            } else if (val < minValue) {
                return 1;
            } else {
                return 0;
            }
        } else if (op == Op.LESS_THAN) {
            return 1 - estimateSelectivity(Op.GREATER_THAN_OR_EQ, val);
        } else if (op == Op.LESS_THAN_OR_EQ) {
            return 1 - estimateSelectivity(Op.GREATER_THAN, val);
        } else if (op == Op.GREATER_THAN_OR_EQ) {
            return estimateSelectivity(Op.GREATER_THAN, val)
                    + estimateSelectivity(Op.EQUALS, val);
        } else if (op == Op.NOT_EQUALS) {
            return 1 - estimateSelectivity(Op.EQUALS, val);
        } else if (op == Op.LIKE) {
            return avgSelectivity();
        } else {
            throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * @return the average selectivity of this histogram.
     * 
     *         This is not an indispensable method to implement the basic join
     *         optimization. It may be needed if you want to implement a more
     *         efficient optimization
     */
    public double avgSelectivity() {
        double avg = 0;
        for (int i = 0; i < numBuckets; i++) {
            double height = buckets[i];
            // probability * number
            avg += height / count * height;
        }
        return avg;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return "<IntHistogram (unimplemented)>";
    }
}
