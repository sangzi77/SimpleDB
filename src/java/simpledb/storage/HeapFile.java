package simpledb.storage;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
/*heapfile和btreefile都实现了dbfile接口，所以都可以用来操作文件*/

//HeapFile对象包含一组“物理页”，每一个页大小固定，
// 大小由BufferPool.DEFAULT_PAGE_SIZE定义，页内存储行数据。
// 在SimpleDB中，数据库中每一个表对应一个HeapFile对象，
// HeapFile对象中的物理页的类型是HeapPage，
// 物理页是存储在buffer pool中，通过HeapFile类读写。
//
//HeapFile类实现了DbFile的接口
    /*这就解释了为什么在Lab1的时候那个图显示的是dbfile去操作，其实是heapfile操作，因为实现了dbfile接口*/

//HeapPage负责找到目标行，然后对其做删除或者插入。而HeapFile则通过BufferPool拿到Page，再用page的Insert和Delete方法插入删除。
// 倒是脏页那边一开始绕进去了，没对修改后的脏页Page进行返回，并写回（存入）BufferPool。

    //数据库中一个表对应一个heapfile对象
public class HeapFile implements DbFile {

    private File file;//表中的内容
    private TupleDesc tupleDesc;//表的属性行

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    //返回表的内容
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    //返回标识此表的唯一id
    //getId()：由绝对路径生成id

    //应该确保每个表都有一个唯一的id，对于特定的HeapFile文件返回相同的值。
    // 文档建议使用heapfile文件的绝对文件名进行hash
    public int getId() {
        // some code goes here
        // 文件的绝对路径，取hash。独一无二的id
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */

    //返回表的属性行
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    //readPage()：读取页面
    //从磁盘上读取一个页面。大致思路如下：首先确定偏移值（页数乘单页大小），
    // 然后初始化一张空 page ，最后根据偏移值将空 page 填满。
    // 最后跑通 HeapFileReadTest 这个类。

    /*在读取page时，readPage()方法仅会被BufferPool中的getPage()方法调用，
    ！！！！！
    “而在其他位置需要获取page时，均要通过BufferPool调用。这也是BufferPool的意义所在。
    ！！！！！
    用RandomAccessFile去读文件，通过seek(offset)可以直接访问偏移量所对应的位置而不用从头进行查找，
    然后read(buf)，将偏移量后面buf.length长度的数据保存到buf中。*/
    public Page readPage(PageId pid) {
        // some code goes here
        //表id
        int tableId = pid.getTableId();
        //该表所处的页码
        int pageNumber = pid.getPageNumber();
        //tableId和pageNumber 用于获取heapPageId

        int pageSize = Database.getBufferPool().getPageSize();
        //page是数据库处理的最基本数据单元，每次读取一个page
        long offset = pageNumber * pageSize;
        byte[] data = new byte[pageSize];
        RandomAccessFile rfile = null;
        try{
            rfile = new RandomAccessFile(file,"r");
            rfile.seek(offset);//通过seek(offset)可以直接访问偏移量所对应的位置
            rfile.read(data);//read(buf)，将偏移量后面buf.length长度的数据保存到buf中。
            //tableId:page所在表的id
            //pageNumber:page的序号
            HeapPageId heapPageId = new HeapPageId(tableId,pageNumber);
            HeapPage heapPage = new HeapPage(heapPageId,data);
            return heapPage;

        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("HeapFile: readPage: file not found");
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format("HeapFile: readPage: file with offset %d not found",offset));
        } finally {
            try {
                rfile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    // see DbFile.java for javadocs
    //将page写入磁盘

    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        //lab2
        int pgNo = page.getId().getPageNumber();

        if (pgNo > numPages()){
            throw new IllegalArgumentException();
        }

        int pageSize = BufferPool.getPageSize();

        //write io
        RandomAccessFile f = new RandomAccessFile(this.file, "rw");
        //set offset
        f.seek(pgNo*pageSize);
        //write
        byte[] data = page.getPageData();
        f.write(data);
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    //返回表中page的数目
    //numsPage()：计算文件有多少页
    public int numPages() {
        // some code goes here
        // 文件长度 / 每页的字节数
       int res = (int) Math.floor(file.length() *1.0 /BufferPool.getPageSize());
       return res;
    }

    // see DbFile.java for javadocs
    //将tuple插入到HeapFile中的page,如果HeapFile中的page都已经满了，则在HeapFile中创建一个新的Page
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        //存储脏页
        ArrayList<Page> arrayList = new ArrayList<>();

        for(int pgNo=0; pgNo<numPages(); pgNo++){

            //这里的getId（），因为bufferpoll在insertTuple的时候就获取了这个文件了，那么当然可以直接获取到文件的Id
            HeapPageId pageId = new HeapPageId(getId(),pgNo);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().
                    getPage(tid,pageId,Permissions.READ_WRITE);/*在bufferpoll中，获取“独写”权限*/

            if(heapPage.getNumEmptySlots() != 0){//如果页面有空的槽位
                heapPage.insertTuple(t);
                arrayList.add(heapPage);
                return arrayList;//脏页
            }

            //----------------- lab 4 ------------------------
            // 当该 page 上没有空闲空 slot 的时候，释放该 page 上的锁，避免影响其他事务访问
            else{
                Database.getBufferPool().unsafeReleasePage(tid, pageId);
            }
        }

        //页面没有空槽了
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file,true));
        //新建一个新的页
        byte[] emptyPageData = HeapPage.createEmptyPageData();//file就是这个heapfile表的内容
        bw.write(emptyPageData);
        //close前会调用flush()刷盘到文件
        bw.close();

        //创建新的page
        //BufferedInputStream和BufferedOutputStream类就是实现了缓冲功能的输入流/输出流。使用带缓冲的输入输出流，效率更高，速度更快。
        //
        //
        //使用步骤：
        //1、创建FileOutputStream对象，构造方法中绑定要输出的目的文件
        //2、创建BufferedOutputStream对象
        //3、使用BufferedOutputStream对象中的方法write，把数据写入到内部缓冲区
        //4、使用BufferedOutputStream对象中的方法flush，把内部缓冲区中的数据，刷新到文件中。
        //5、释放资源（会先调用flush方法刷新数据，可省略）
        //
        //
        //BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("bos.txt")) ;
        ////写数据
        //bos.write("hello".getBytes());
        ////释放资源
        //bos.close();

        // 创建新的页面
        HeapPageId newPageId = new HeapPageId(getId(), numPages() - 1);
        HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, newPageId, Permissions.READ_WRITE);
        newPage.insertTuple(t);
        arrayList.add(newPage);
        return arrayList;//存储脏页
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException, IOException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pageList= new ArrayList<>();

        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(),
                        Permissions.READ_WRITE);/*在bufferpoll中，获取“独写”权限*/
        p.deleteTuple(t);
        pageList.add(p);
        return pageList;
    }

    // see DbFile.java for javadocs
    //回HeapFile中所有的heapPage 中的所有元组的迭代器
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    private static class HeapFileIterator implements DbFileIterator {
        private final HeapFile heapFile;
        private final TransactionId tid;
        // 元组迭代器
        private Iterator<Tuple> iterator;
        private int whichPage;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException, IOException {
            // 获取第一页的全部元组
            whichPage = 0;
            iterator = getPageTuple(whichPage);
        }

        // 获取当前页的所有行
        private Iterator<Tuple> getPageTuple(int pageNumber) throws TransactionAbortedException, DbException, IOException {
            // 在文件范围内
            if (pageNumber >= 0 && pageNumber < heapFile.numPages()) {
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNumber);
                // 从缓存池中查询相应的页面 读权限
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }
            throw new DbException(String.format("heapFile %d not contain page %d", pageNumber, heapFile.getId()));
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException, IOException {
            // 如果迭代器为空
            if (iterator == null) {
                return false;
            }
            // 如果已经遍历结束
            if (!iterator.hasNext()) {
                // 是否还存在下一页，小于文件的最大页
                while (whichPage < (heapFile.numPages() - 1)) {
                    whichPage++;
                    // 获取下一页
                    iterator = getPageTuple(whichPage);
                    if (iterator.hasNext()) {
                        return iterator.hasNext();
                    }
                }
                // 所有元组获取完毕
                return false;
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // 如果没有元组了，抛出异常
            if (iterator == null || !iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            // 返回下一个元组
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException, IOException {
            // 清除上一个迭代器
            close();
            // 重新开始
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }
}

