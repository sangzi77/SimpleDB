package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
//与Predicate类似，区别在于其比较给定的两条记录指定字段
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */


    //JoinPredicate和Predicate其实很像，只不过比较的是两个tuple，也就是用于表join操作时的比较
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        this.field1 = field1;//比较字段1
        this.op = op;//比较符号
        this.field2 = field2;//比较字段2
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    private int field1;//字段一
    private Predicate.Op op; //比较符号
    private int field2; //字段二
    //两个元组进行比较
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        //取出的是两个filed序号分别进行比较
        return t1.getField(field1).compare(op,t2.getField(field2));
    }
    
    public int getField1()
    {
        // some code goes here
        return field1;
    }
    
    public int getField2()
    {
        // some code goes here
        return field2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return op;
    }
}
