package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.io.IOException;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */

/*Filter实现了Operator接口。
根据Predicate的判读结果，得到满足条件的tuples。
实现了where age > 18这样的操作。*/

//过滤器,该运算符过滤出满足条件的表记录，基于Predicate比较
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate predicate;//对Predicate封装，通过predicate实现对每一个tuple的过滤操作
    private OpIterator child; //待过滤的tuples的迭代器

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    //初始化方法
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.predicate = p;
        this.child = child;
    }

    //返回predicate
    public Predicate getPredicate() {
        // some code goes here
        return predicate;
    }

    //返回代过滤元组的属性
    //用child.getTupledesc()即可
    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    //Filter是项目中的Operator类的子类， 需要执行super.open()
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException, IOException {
        // some code goes here
        child.open();//这里不懂...
        super.open();
    }
    //对child 和 super进行close:这里也不理解
    public void close() {
        // some code goes here
        child.close();//这里也不懂为什么。
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException, IOException {
        // some code goes here
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    //比较是否符合条件，使用 filter过滤
    //返回过滤后的tuple
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException, IOException {
        // some code goes here
        while (child.hasNext()){
            Tuple t = child.next();
            if (predicate.filter(t)){
                return t;
            }
        }
        return null;
    }

    //返回待过滤的tuples
    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    //重置待过滤的tuples
    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }

}
