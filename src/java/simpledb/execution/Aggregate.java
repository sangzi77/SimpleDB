package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
//Aggregate实现Operator接口，是聚合操作类。比如select count(*) from xx group by xxx
//IntegerAggregator和StringAggregator其实都是Aggregate的辅助类
//区别在于聚合操作字段的属性，如果是Integer，可以有count、sum、max、min、avg操作；如果是String，只有count操作


    //最新：实现聚合分组，其实就是根据需要的类型（int or string）去调用IntegerAggregator和StringAggregator
public class Aggregate extends Operator {

    //聚合运算 count、sum、avg、min、max
    //分组：group by
    private static final long serialVersionUID = 1L;
//一条带有聚合函数的分组查询语句是怎样实现的？
//0.客户端发起请求，sql语句(假如我们有客户端和服务端)；
//1.sql解析器进行解析，得出需要从member表中获取数据，分组字段是country(gbField = 1)，聚合字段是fee(aggField = 2)，聚合运算符op=SUM；
//2.根据member表的id，调用Database.getCatalog().getDatabaseFile(tableid)获取数据表文件HeapFile，调用HeapFile的iterator方法获取所有表记录，即数据源child；
//3.根据gbField、aggField、op、child创建Aggregate，Aggregate构造器中会根据gbField、aggField、op创建出聚合器IntegerAggregator、聚合结果元组的描述信息td；
//4.调用Aggregate的open方法（这里记住Aggregate本身也是迭代器，open后才能next），在open方法中会不断的从数据源child取出记录，并调用聚合器的mergeTupleIntoGroup进行聚合运算；运算结束后通过聚合器的iterator方法生成结果迭代器it
//5.不断从迭代器it取出结果并返回给客户端
    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */

    // 需要聚合的 tuples
    private OpIterator child;
    // 聚合字段
    private final int afield;
    // 分组字段
    private final int gfield;
    // 运算符
    private Aggregator.Op aop;

    // 进行聚合操作的类
    private Aggregator aggregator;
    // 聚合结果的迭代器
    private OpIterator opIterator;
    // 聚合结果的属性行
    private TupleDesc tupleDesc;


    //Aggregation operator用于计算一个Aggregate（e.g. sum,avg,max,min），我们需要对一列数据支持聚合。
    // 构造函数有四个参数，第一个参数是OpIterator类型的 child，用于不断提供tuples；
    // 第二个参数是 int 类型的 afield，标识着我们需要聚合的列；
    // 第三个参数是 int 类型的gfield，标识着结果中我们需要group by 的列；
    // 第四个参数是 Aggregator.Op类型的aop，是我们需要使用的Aggregation operator。

    //一条带有聚合函数的分组查询语句是怎样实现的？
    //
    //0.客户端发起请求，sql语句(假如我们有客户端和服务端)；
    //
    //1.sql解析器进行解析，得出需要从member表中获取数据，分组字段是country(gbField = 1)，聚合字段是fee(aggField = 2)，聚合运算符op=SUM；
    //
    //2.根据member表的id，调用Database.getCatalog().getDatabaseFile(tableid)获取数据表文件HeapFile，调用HeapFile的iterator方法获取所有表记录，即数据源child；
    //
    //3.根据gbField、aggField、op、child创建Aggregate，Aggregate构造器中会根据gbField、aggField、op创建出聚合器IntegerAggregator、聚合结果元组的描述信息td；
    //
    //4.调用Aggregate的open方法（这里记住Aggregate本身也是迭代器，open后才能next），在open方法中会不断的从数据源child取出记录，并调用聚合器的mergeTupleIntoGroup进行聚合运算；运算结束后通过聚合器的iterator方法生成结果迭代器it
    //
    //5.不断从迭代器it取出结果并返回给客户端


    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        // 判断是否分组
        Type gfieldtype = gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield);

        // 创建聚合器
        if(child.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE){
            this.aggregator = new StringAggregator(gfield, gfieldtype, afield, aop);
        }
        else{
            this.aggregator = new IntegerAggregator(gfield, gfieldtype, afield, aop);
        }

        // 组建 TupleDesc
        List<Type> typeList = new ArrayList<>();
        List<String> nameList = new ArrayList<>();

        if(gfieldtype != null){
            typeList.add(gfieldtype);
            nameList.add(child.getTupleDesc().getFieldName(gfield));
        }

        typeList.add(child.getTupleDesc().getFieldType(afield));
        nameList.add(child.getTupleDesc().getFieldName(afield));

        if(aop.equals(Aggregator.Op.SUM_COUNT)){
            typeList.add(Type.INT_TYPE);
            nameList.add("COUNT");
        }
        this.tupleDesc = new TupleDesc(typeList.toArray(new Type[typeList.size()]), nameList.toArray(new String[nameList.size()]));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */

    //如果这个Aggregate伴随有 group by，返回group by的field 的索引。
    public int groupField() {
        // some code goes here
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    //如果这个Aggregate伴随有 groupby，返回groupby的field 的Name。
    public String groupFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        if (this.gfield == -1){
            return tupleDesc.getFieldName(0);
        }else{
            return tupleDesc.getFieldName(1);
        }
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }
    //open()函数中，根据传入的数据迭代器迭代每一个tuple，用聚合器进行聚合，以此生成一个最终结果的迭代器。
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException, IOException {
        // some code goes here

        //聚合所有的tuple
        child.open();
        while (child.hasNext()){
            aggregator.mergeTupleIntoGroup(child.next());
        }
        // 获取聚合后的迭代器
        opIterator = aggregator.iterator();
        // 查询
        //至于这里为何要有一个opiterrator应该是因为这个Aggregate继承了Operator，然后把处理好的聚合结果给封装到Aggregate的opIterator，然后在测试用例中
        //就可以直接使用Aggrefate的iterrator直接打印所有的结果了。
        opIterator.open();
        // 使父类状态保持一致
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    //返回下一个tuple。如果有groupby field，
    // 那么第一个field是我们group的field，第二个field是计算的aggregate结果；
    // 如果没有groupby field，只需要返回结果。
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, IOException {
        // some code goes here
        if (opIterator.hasNext()){
            return opIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException, IOException {
        // some code goes here
        this.child.rewind();
        opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    //返回这个aggregate计算结果tuple的TupleDesc。
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        opIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
        Type gfieldtype = child.getTupleDesc().getFieldType(gfield);

        // 组建 TupleDesc
        List<Type> typeList = new ArrayList<>();
        List<String> nameList = new ArrayList<>();

        // 加入分组后的字段
        if(gfieldtype != null){
            typeList.add(gfieldtype);
            nameList.add(child.getTupleDesc().getFieldName(gfield));
        }

        // 加入聚合字段
        typeList.add(child.getTupleDesc().getFieldType(afield));
        nameList.add(child.getTupleDesc().getFieldName(afield));

        //因为只有string有count，int类型没有count所以要单独判断
        if(aop.equals(Aggregator.Op.SUM_COUNT)){
            typeList.add(Type.INT_TYPE);
            nameList.add("COUNT");
        }

        this.tupleDesc = new TupleDesc(typeList.toArray(new Type[typeList.size()]), nameList.toArray(new String[nameList.size()]));
    }
}
