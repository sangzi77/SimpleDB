package simpledb.common;

import simpledb.common.Type;
import simpledb.optimizer.TableStats;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * 目录跟踪数据库中的所有可用表及其关联schema
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 目前，这是一个存根目录，必须由
 * 用户程序才能使用--最终，应该转换
 * 到从磁盘读取目录表的目录。
 * 
 * @Threadsafe
 */
public class Catalog {

    /*Catelog是Table的集合，然后每个Table有DbFile, PrimaryKeyField, Name
    全局catalog是分配给整个SimpleDB进程的Catalog类一个实例，可以通过方法Database.getCatalog()获得，global buffer pool可以通过方法Database.getBufferPool()获得。*/

    /*
    * Catelog有一个成员变量保存了所有的表信息，每张表保存的信息如下：
      DbFile是磁盘上数据库文件的接口。每个表都由一个 独立的DbFile 表示。 每个文件都有一个唯一的 id和保存了元信息，DbFiles 可以获取页面并遍历元组， DbFiles 通常通过缓冲池访问，而不是直接由操作员访问；
      name就是表的名字；
      pkeyField就是主键的字段名称。
    * */

    //这个hashTable就是Catalog
    private ConcurrentHashMap<Integer,Table> hashTable;//表id与表的映射

    private HashMap<String,Integer> hashMap;//表名字与表id的映射，方便后续通过表名获取表id

    private static class Table{
    /*如果定义了private static final long serialVersionUID = 1L，那么如果你忘记修改这个信息，而且你对这个类进行修改的话，这个类也能被进行反序列化，而且不会报错。 一个简单的概括就是，如果你忘记修改，那么它是会版本向上兼容的。 如果没有定义一个名为serialVersionUID，类型为long的变量，Java序列化机制会根据编译的class自动生成一个serialVersionUID，即隐式声明。*/
    private static final long serialVersionUID = 1L;

        //数据库中每张表对应着一个DbFile
    public DbFile dbFile;//DbFile中提供了getId()方法，可以获取此Dbfile对应表的tableid。这个id并不是顺序生成，后续exercise中通过file.getAbsoluteFile().hashCode()生成的
    public String tableName;
    public String pk;

    //辅助类Table,包含参数tableName、primartKey（表中的主键）、DbFile dbFile （用于存储表的内容）
    public Table(DbFile dbFile, String tableName, String pk) {
        this.dbFile = dbFile;
        this.tableName = tableName;
        this.pk = pk;
    }

        @Override
        public String toString() {
            return "Table{" +
                    "dbFile=" + dbFile +
                    ", tableName='" + tableName + '\'' +
                    ", pk='" + pk + '\'' +
                    '}';
        }
    }


    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    //构造方法
    //这个hashtable就是catalog
    public Catalog() {
        // some code goes here
        hashTable =  new ConcurrentHashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    //向CataLog中添加表
    //数据库中每张表对应着一个DbFile,那么dbfile.getid()就是获取表的Id
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        Table table = new Table(file, name, pkeyField);
        hashTable.put(file.getId(),table);//DbFile中提供了getId()方法，可以获取此Dbfile对应表的tableid。这个id并不是顺序生成，后续exercise中通过file.getAbsoluteFile().hashCode()生成的
    }
    //向CataLog中添加表
    //主键可为空
    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    //向CataLog中添加表
    //name为空时随机一个UUID作为其name。
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    //通过表名获取表id
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        //searchValues:遍历value(所有表？),使用searchFunction查找返回非空结果，如果没有则返回null。
       Integer res = hashTable.searchValues(1,value->{
            if (value.tableName.equals(name)){
                return value.dbFile.getId();
            }
            return null;
        });
        if (res != null){
            return res;
        }else {
            throw new NoSuchElementException("not found id for table "+ name);
        }
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    //通过表id 获得表的TupleDesc
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        Table table = hashTable.getOrDefault(tableid, null);
        if (table != null){
            return table.dbFile.getTupleDesc();
        }else {
            throw new NoSuchElementException("not found tuple desc for table" + tableid);
        }
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    //通过表id 获得表的内容DbFile
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        Table table = hashTable.getOrDefault(tableid, null);
        if (table !=null){
            return table.dbFile;
        }else {
            throw new NoSuchElementException("not found tuple desc for table" + tableid);
        }
    }
    //通过表id 获得表的主键
    public String getPrimaryKey(int tableid) {
        // some code goes here
        Table table = hashTable.getOrDefault(tableid, null);
        if (table != null){
            return table.pk;
        }else {
            throw new NoSuchElementException("not found primary key for table" + tableid);
        }
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        Iterator<Integer> iterator = hashTable.keySet().iterator();//把hashTable里的键全设置到一个数组中，并迭代这个数组。
        return iterator;
    }
    //通过表id 获得表名
    public String getTableName(int id) {
        // some code goes here
        Table table = hashTable.getOrDefault(id, null);
        if (table != null){
            return table.tableName;
        }else {
            throw new NoSuchElementException("not found table name for table"+ id);
        }
    }
    
    /** Delete all tables from the catalog */
    //清空catalog
    public void clear() {
        // some code goes here
        hashTable.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * 从文件中读取架构并在数据库中创建适当的表。
     * @param catalogFile
     */
    //利用正则化从file中读取表的结构，并在数据库中创建所有合适的表。
    //这个还不是很理解，，，
    public void loadSchema(String catalogFile) {
        String line = "";
        //根目录
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            //读取catelogFile
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                //假设行的格式名称（字段类型、字段类型…）
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

