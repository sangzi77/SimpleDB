package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */

/**
 * exercise4要求我们实现Insertion and deletion两个操作符，
 * 实际上就是两个迭代器，实现方式与exercise1相似，将传入的数据源进行处理，
 * 并返回处理结果，而处理并返回结果一般都是写在fetchNext中。
 * 这里的处理结果元组，只有一个字段，那就是插入或删除影响的行数，与MySQL相似。
 * 具体实现插入和删除，需要调用我们exercise3实现的插入删除元组相关方法。
 * ————————————————
 */

//实现了Operator接口，调用BufferPool的insertTuple()方法向给定的表中插入tuple
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    // 插入的元组 迭代器
    private OpIterator child;
    // 要插入的表位置
    private final int tableId;

    // 标志位，避免 fetchNext 无限往下取
    private boolean inserted;
    // 返回的 tuple (用于展示插入了多少的 tuples)
    private final TupleDesc tupleDesc;


    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if(!child.getTupleDesc().equals(Database.getCatalog().getDatabaseFile(tableId).getTupleDesc())){
            throw new DbException("插入的类型错误");
        }
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"the number of inserted tuple"});
        this.inserted = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException, IOException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException, IOException {
        // some code goes here
        child.rewind();
        inserted = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, IOException {
        // some code goes here
        // 还未插入
        if(!inserted){
            // 计算插入了多少行
            inserted = true;
            int count = 0;
            while (child.hasNext()){
                Tuple tuple = child.next();
                try{
                    Database.getBufferPool().insertTuple(tid, tableId, tuple);
                    count++;
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            // 返回插入的次数 所组成的元组
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(count));
            return tuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
