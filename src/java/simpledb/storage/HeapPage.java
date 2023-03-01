package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */


//HeapPage负责找到目标行，然后对其做删除或者插入。
// 而HeapFile则通过BufferPool拿到Page，再用page的Insert和Delete方法插入删除。
// 倒是脏页那边一开始绕进去了，没对修改后的脏页Page进行返回，并写回（存入）BufferPool。


/*HeapPage中包含多个slot和一个header，每个slot是留给一行的位置。
header是每个tuple slot的bitmap。
如果bitmap中对应的某个tuple的bit是1，
则这个tuple是有效的，否则无效（被删除或者没被初始化）。*/
public class HeapPage implements Page {

    private final HeapPageId pid; //pageid

    private final TupleDesc td; //page对应的属性行
    // 槽储存
    private final byte[] header; //slot的bitmap，用于判断slot是否被占用
    // 元组数据
    private final Tuple[] tuples; //page中的元组
    // 槽数
    private final int numSlots;//page中slot的数量

    // 事务 id
    private TransactionId tid;
    // 是否是脏页
    private boolean dirty;

    /**
     * 旧数据，用于undo log
     */
    byte[] oldData;

    private final Byte oldDataLock= (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;//页面id
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());//tupledesc
        this.numSlots = getNumTuples();//获取所有槽位数量
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        // 处理头部
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        // 处理每行
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
     @return the number of tuples on this page
     */
    //返回每个page中最多包含的tuple数
    private int getNumTuples() {
        // some code goes here
        // 计算页面有多少个元组
        // tuple_nums = floor((page_size * 8) / tuple_size * 8 + 1)
        return (int) Math.floor((BufferPool.getPageSize() * 8 * 1.0) / (td.getSize() * 8 + 1));

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    //SimpleDB数据库的每个tuple需要tuple size * 8 bits 的内容大小和1 bit的header大小
    // 因此，在一页中可以包含的tuple数量计算公式是：
    // tuples per page = floor((page size * 8) / (tuple size * 8 + 1))。
    // 其中，tuple size是页中单个tuple 的bytes大小。

    //返回page中的header的大小
    private int getHeaderSize() {
        // some code goes here
        // headerBytes = ceiling(tuplePerPage / 8);
        return (int) Math.ceil(getNumTuples() * 1.0 / 8);
    }

    /** Return a view of this page before it was modified
     -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */

    //返回pageid
    public HeapPageId getId() {
        // some code goes here
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }
        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    //删除page中的tuple，同时修改该slot对应的bitmap（通过markSlotUsed方法），表示该slot已为空
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        // 查看属性是否匹配
        int tupleId = t.getRecordId().getTupleNumber();
        // 页面已被删除， 类型不相同， 页面不相同
        if(tuples[tupleId] == null || !t.getTupleDesc().equals(td) || !t.getRecordId().getPageId().equals(pid)){
            throw new DbException("this tuple is not on this page");
        }
        if(!isSlotUsed(tupleId)){
            throw new DbException("tuple slot is already empty");
        }
        // 标记未被使用
        markSlotUsed(tupleId, false);
        // 删除插槽内容
        tuples[tupleId] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        // 查看 Page 是否已满
        if(getNumEmptySlots() == 0){
            throw new DbException("当前页已满");
        }

        // 查看属性是否匹配
        if(!t.getTupleDesc().equals(td)){
            throw new DbException("类型不匹配");
        }

        // 查询 tuple
        for (int i = 0; i < numSlots; i++) {
            // 查看未被使用的槽
            if(!isSlotUsed(i)){
                // 标记使用
                markSlotUsed(i, true);
                // 设置路径
                t.setRecordId(new RecordId(pid, i));
                // 放入槽位
                tuples[i] = t;
                return ;
            }
        }

    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    //修改脏页标志位
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        // not necessary for lab1
        this.dirty = dirty;
        this.tid = tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    //判断该page是否为dirty page如果是则返回产生该脏页的事务id
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        if(dirty){
            return tid;
        }
        return null;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    //返回page中空slot的数量
    public int getNumEmptySlots() {
        // some code goes here
        int count = 0;
        for (int i = 0; i < numSlots; i++) {
            if(!isSlotUsed(i)){
                count++;
            }
        }
        return count;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    //判断第i个slot是否为空
    public boolean isSlotUsed(int i) {
        // some code goes here
        // 槽位
        int quot = i / 8;
        // 偏移
        int move = i % 8;
        // 获得对应的槽位
        int bitidx = header[quot];
        // 偏移 move 位，看是否等于 1
        return ((bitidx >> move) & 1) == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    //修改page中的header,在value == true的情况下，在第i个槽位添加已使用，==false的话在第i个槽位位删除
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        // 找到槽位
        int slot = i / 8;
        // 偏移
        int move = i % 8;
        // 掩码
        byte mask = (byte) (1 << move);
        // 更新槽位
        if(value){
            // 标记已被使用，更新 0 为 1
            header[slot] |= mask;
        }else{
            // 标记为未被使用，更新 1 为 0
            // 除了该位其他位都是 1 的掩码，也就是该位会与 0 运算, 从而置零
            header[slot] &= ~mask;
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    //返回HeapPage中所有元组的迭代器（不能包括空slot）
    public Iterator<Tuple> iterator() {
        // some code goes here
        // 获取已使用的槽对应的数
        ArrayList<Tuple> res = new ArrayList<>();
        for (int i = 0; i < numSlots; i++) {
            if(isSlotUsed(i)){
                res.add(tuples[i]);
            }
        }
        return res.iterator();
    }
}

