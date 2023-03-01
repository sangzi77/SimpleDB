package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
//统计直方图 对于某一个限制条件占总体的一个比例

    //每个柱子高度的计算公式：buckets[index] / width
    //每个柱子的宽度计算公式：value - index * width - min

public class IntHistogram {

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
    //也就是说，我们需要去计算一个选择率
    //
    //步骤：
    //1、首先需要把值传进来放入 buckets
    //2、然后根据传进来的运算符，计算值所占的比例
    //3、计算的过程
    //	首先统计当前值 value 前的所有总数，也就是通过buckets[] 去统计所有的总数
    //	然后就去计算value所占的数量，其实就是计算面积，index 是 value 所在的索引柱
    //	buckets[index] / width 计算出当前柱的高度（每个柱子的宽度是相同的，整个柱子的面积除于宽度就是高度）
    //	value - index * width - min 计算当前柱所占的宽度（减去当前所有柱子前的宽度再减去最小值，也就获得了所占区域的宽度）
    //	然后对面积进行计算 (1.0 * buckets[index] / width) * (value - index * width - min)
    //	这里 1.0 的目的是 避免精度丢失

    private int[] buckets; //直方图
    private int min;//边界最小值
    private int max;//边界最大值
    private double width;//长度
    private int tuplesCount = 0;//行数

    //构造函数，初始化桶的数量、最大值、最小值
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets]; //直方图
        this.min = min; // 边界最小值
        this.max = max;//边界最大值
        this.width = (max - min + 1.0) / buckets;//长度
        this.tuplesCount = 0;//行数
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    //向直方图中添加数据
    public void addValue(int v) {
    	// some code goes here
        if (v >= min && v<=max){
            buckets[getIndex(v)]++;
            tuplesCount++;
        }
    }
    //根据value获得桶的序号
    private int getIndex(int v){
        return (int)((v-min)/width);
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
    //根据运算符和值 估计选择率
    //例如 op 是大于， v 是 3 ，那么就是计算横坐标大于 3 所有 tuple 个数除以总 tuple 个数(ntuple)。
    // 也就是大于 3 tuple 占总 tuple 的百分比。

    //返回指定判断条件下（谓词(大于/等于/小于)+值）的选择率
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        switch (op){
            case LESS_THAN:
                if (v <= min){
                    return 0.0;
                }
                else if (v >= max){
                    return 1.0;
                }
                else {
                    int index = getIndex(v);
                    double tuples = 0;
                    for (int i = 0; i < index; i++) {
                        tuples += buckets[i];
                    }
                    // 索引所在柱的高度 * （当前值 - 该柱前的宽度） < 这个也就是当前柱所占的宽度
                    tuples += (1.0 * buckets[index] / width) * (v - index * width - min);
                    return tuples / tuplesCount;
                }
            case GREATER_THAN:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
            case EQUALS:
                return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
            default:
                throw new IllegalArgumentException("Operation is illegal");
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
        int cnt = 0;
        for(int bucket : buckets){
            cnt += bucket;
        }
        if(cnt == 0) return 0;
        return (cnt * 1.0 / tuplesCount);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("IntHistogram(buckets = %d, min = %d, max = %d)", buckets.length, min, max);
    }
}
