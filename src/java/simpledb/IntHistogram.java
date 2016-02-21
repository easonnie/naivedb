package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int ntup;

    private int[] hist;
    //private double[] range_left;
    //private double[] range_right;
    private double width;


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
        this.min = min;
        this.max = max;
        this.buckets = buckets;
        this.hist = new int[buckets];

        this.width = (double) (max - min) / buckets;

        for (int h : this.hist) {
            h = 0;
        }

        this.ntup = 0;


    }

    public void test() {
        System.out.println("Max:" + max);
        System.out.println("Min:" + min);
        System.out.println("Width: " + width);
        System.out.println("ntup: " + ntup);
        System.out.println("---------hist---------");

        for (int i = 0; i < buckets; i++) {
            double right = (min + width * i);
            double left = (min + width * (i + 1));
            System.out.println("( " + right + " to " + left + " ):" + hist[i] + "  Prob:" + (hist[i] / (double)ntup));
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        this.ntup ++;
        if (v == min) {
            hist[0]++;
        } else if (v == max) {
            hist[buckets - 1]++;
        } else {
            int which = (int) (Math.ceil((v - min) / width)) - 1;
            hist[which] ++;
        }
    }

    private int which(int v) {
        if (v == min) {
            return 0;
        } else if (v == max) {
            return buckets - 1;
        } else {
            return (int) (Math.ceil((v - min) / width)) - 1;
        }
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

        int height;

        double count_width;
        if (width > 1.0) {
            count_width = width;
        } else {
            count_width = 1.0;
        }

    	// some code goes here
        if (op == Predicate.Op.EQUALS || op == Predicate.Op.LIKE) {
            if (v < min || v > max) {
                return 0.0;
            } else {
                int w = which(v);
                height = hist[w];
                return (height / count_width) / ntup;
            }
        } else if (op == Predicate.Op.GREATER_THAN) {
            if (v > max) {
                return 0.0;
            } else if (v < min) {
                return 1.0;
            } else {
                int w = which(v);
                height = hist[w];
                double b_right = min + width * (w + 1);
                double b_f = height / (double) ntup;

                double b_part = (b_right - v) / width;
                if (width < 1.0) {
                    b_part = 0.0;
                }
                double result = b_f * b_part;

                for (int i = w + 1; i < buckets; i++) {
                    result = result + hist[i] / (double) ntup;
                }

                return result;
            }
        } else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            double d_ntup = (double) ntup;
            if (v > max) {
                return 0.0;
            } else if (v <= min) {
                return 1.0;
            } else {
                int w = which(v);
                height = hist[w];
                double e_value = (height / count_width) / d_ntup;

                double b_right = min + width * (w + 1);
                double b_f = height / d_ntup;
                double b_part = (b_right - v) / width;
                if (width < 1.0) {
                    b_part = 0.0;
                }
                double g_value = b_f * b_part;

                for (int i = w + 1; i < buckets; i++) {
                    g_value = g_value + hist[i] / d_ntup;
                }

                return e_value + g_value;
            }
        } else if (op == Predicate.Op.LESS_THAN) {
            double d_ntup = (double) ntup;
            if (v > max) {
                return 1.0;
            } else if (v < min) {
                return 0.0;
            } else {
                int w = which(v);
                height = hist[w];

                double b_left = min + width * w;
                double b_f = height / d_ntup;
                double b_part = (v - b_left) / width;
                if (width < 1.0) {
                    b_part = 0.0;
                }
                double l_value = b_f * b_part;

                for (int i = 0; i < w; i++) {
                    l_value = l_value + hist[i] / d_ntup;
                }

                return l_value;
            }
        } else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            double d_ntup = (double) ntup;
            if (v > max) {
                return 1.0;
            } else if (v < min) {
                return 0.0;
            } else {
                int w = which(v);
                height = hist[w];
                double e_value = (height / count_width) / d_ntup;

                double b_left = min + width * w;
                double b_f = height / d_ntup;
                double b_part = (v - b_left) / width;
                if (width < 1.0) {
                    b_part = 0.0;
                }
                double l_value = b_f * b_part;

                for (int i = 0; i < w; i++) {
                    l_value = l_value + hist[i] / d_ntup;
                }

                return l_value + e_value;
            }
        } else if (op == Predicate.Op.NOT_EQUALS) {
            double d_ntup = (double) ntup;
            if (v > max) {
                return 1.0;
            } else if (v < min) {
                return 1.0;
            } else {
                int w = which(v);
                height = hist[w];
                double e_value = (height / count_width) / d_ntup;

                return 1.0 - e_value;
            }
        }

        return -1.0;
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
