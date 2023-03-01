package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.io.IOException;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private  TransactionId tid;//事务id
    private int tableid; //要查询的表id
    private String tableAlias; //表别名
    //遍历表中的所有tuple
    private DbFileIterator iterator; //引入了DbfileIterrator（迭代器用于检索），只需要封装一下即可


    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */

    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid; //事务id
        this.tableid = tableid;//扫描的表id
        this.tableAlias = tableAlias;//表的别名
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    //通过catalog获取table的名称
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    //查找新的表
    //重新对tableid、tableAlias赋值
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    //开启迭代器，也就是获取迭代器
    public void open() throws DbException, TransactionAbortedException, IOException {
        // some code goes here
        // 查询目录 -》 根据表id查询相应的 DBFile -> 获取迭代器
        //这里的迭代器其实就是使用之前写的 HeapFileIterator，所以主要代码还是在之前的内部类中，这里只是调用
        iterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    //过Catelog获取TupleDesc，在tupleDesc中的fileName前添加表的别名
    //类似as ? >>> a.name a.age a.weight ??
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableid);
        String prefix = tableAlias != null? tableAlias : "null";
        // 遍历，添加前缀
        int len = tupleDesc.numFields();
        Type[] types = new Type[len];
        String[] fieldNames = new String[len];
        for (int i = 0; i < len; i++) {
            types[i] = tupleDesc.getFieldType(i);
            fieldNames[i] = prefix + "." + tupleDesc.getFieldName(i);
        }
        return new TupleDesc(types, fieldNames);//组装成新的tupledesc,types原装不动，fieldNames前面有prefix(前缀)
    }


    public boolean hasNext() throws TransactionAbortedException, DbException, IOException {
        // some code goes here
        if(iterator == null){
            return false;
        }
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException, IOException {
        // some code goes here
        if(iterator == null){
            throw new NoSuchElementException("No Next Tuple");
        }
        Tuple tuple = iterator.next();
        if(tuple == null){
            throw new NoSuchElementException("No Next Tuple");
        }
        return tuple;
    }

    public void close() {
        // some code goes here
        iterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException, IOException {
        // some code goes here
        iterator.rewind();
    }
}
