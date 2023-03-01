package simpledb.Test;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;

public class Test {

    public static void main(String[] args) throws IOException {
        // 创建模式头部
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] name = new String[]{"field0", "field1", "field2", "field3"};
        /*
        *  filed0(int) filed1(int) field2(int) filed3(int)
        *  1           1           1           1
        *  2           2           2           2
        *  3           4           4           5
        *
        * */
        TupleDesc tupleDesc = new TupleDesc(types, name);
        //转换为二进制文件
        HeapFileEncoder.convert(new File("C:\\Users\\12036\\Desktop\\test.dat"), new File("some_date_file.dat"), BufferPool.getPageSize(), 4, types);
        //二进制文件变成文件
        File file = new File("some_date_file.dat");
        // 创建 table 文件
        //文件转为heapfile类型文件
        HeapFile heapFile = new HeapFile(new File("some_date_file.dat"), tupleDesc);

        // 将table 文件 写入日志，表名test
        Database.getCatalog().addTable(heapFile, "test");

        // 创建事务 id
        TransactionId transactionId = new TransactionId();
        // 根据表 id 查询表
        SeqScan scan = new SeqScan(transactionId, heapFile.getId());

        try{
            scan.open();
            while (scan.hasNext()){
                Tuple tuple = scan.next();
                System.out.println(tuple);
            }
            scan.close();
            Database.getBufferPool().transactionComplete(transactionId);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
