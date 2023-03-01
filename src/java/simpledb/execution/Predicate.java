package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
//将元组与指定的字段值进行比较

    //这个使用心得：在测试用例中Predicate p = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(i));//创建一个Predicate：下标0的位置，等于，创建一个-1字段
   // 像上面那样自己new一个Prediccate然后，再用Utility.getHeapTuple(1)创建一个字段，然后再调用p（这个p就是上面创建的Predicate）.filter(Utility.getHeapTuple(i))进行比较
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;


    /** Constants used for return codes in Field.compare */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;
//==        >           <           <=              >=         LIKE主要针对字符串     !=
        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         * 
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }

    private int field;//要比较的字段数
    private Op op; //比较符号
    private Field operand;//元组传递的比较字段，一般新建一个IntField用来比较

    /**
     * Constructor.
     * 
     * @param field
     *            field number of passed in tuples to compare against.
     * @param op
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     */
    /*
    private int field;//tuple中与指定字段对应的字段 的 序号
    private Op op; //比较符号
    private Field operand;//元组传递的比较字段，一般新建一个IntField用来比较
    */
    public Predicate(int field, Op op, Field operand) {
        // some code goes here
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    //返回fieldNo
    public int getField()
    {
        // some code goes here
        return field;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
        // some code goes here
        return op;
    }
    
    /**
     * @return the operand
     */
    public Field getOperand()
    {
        // some code goes here
        return operand;
    }
    
    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 
     * @param t
     *            The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */

    //filter(Tuple t)：（自己创建new的时候就创建了）在特定的field、Operation、operand下与tuple t进行比较。
    //Filter：按条件筛选符合的元组；
    //Filter操作是按照一定的条件对数据表中的元组进行过滤，
    //这个判断的过程在SimpleDB中被抽象成了一个叫做Predicate的类，Predicate就是比较tuple某个字段的值与指定的值是否满足判断
    // // 获取字段，传入比较符号，和当前 Field比较
    //    private int field;//要比较的字段数
    //    private Op op; //比较符号
    //    private Field operand;//元组传递的比较字段，一般新建一个IntField用来比较
    public boolean filter(Tuple t) {
        // some code goes here
        return t.getField(field).compare(op,operand);//应该是传入的tuple的字段（与oprand进行比较）oprand时返回true
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        // some code goes here
        String s = String.format("field = %d op = %s operand = %s", field, op.toString(), operand.toString());
        return s;
    }
}
