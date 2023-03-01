package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {


      private TDItem[] tdItems;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     *        *迭代器迭代所有字段TDItems,包含在TupleDesc中.
     * */

    //返回所有属性的迭代器
    public Iterator<TDItem> iterator() {
        // some code goes here
        Iterator<TDItem> iterator = Arrays.asList(tdItems).iterator();
        return iterator;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated(相关的) named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     *
     */
     /*
     *／**
     *使用typeAr创建一个新的TupleDesc。字段的字段长度
     *指定类型，带有关联的命名字段。
     * @param typeAr  *数组，指定字段的数量和类型 TupleDesc。它必须至少包含一个条目。
     * @param fieldAr
     *数组指定字段的名称。注意，名字可能为空。
     * */
    /*
    *  名(typeAr) 类型(fieldAr)
    *   id        int
    *   address   varchar
    *
    * */
    //初始化方法
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        tdItems = new TDItem[typeAr.length];
        for(int i=0;i<typeAr.length;++i) {
            tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */

    //初始化方法
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        tdItems = new TDItem[typeAr.length];
        for(int i=0;i<typeAr.length;++i){
            tdItems[i] = new TDItem(typeAr[i],"");
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    //返回tupledesc中属性的数量
    public int numFields() {
        // some code goes here
        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    /*取得第i列（字段）的属性名称*/
    public String getFieldName(int i) throws NoSuchElementException {
        if(i<0 || i>= tdItems.length){
            throw new NoSuchElementException("pos " + i + " is not a valid index");
        }
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    /*取得第i列（字段）的属性类型*/
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0 || i>=tdItems.length){
            throw new NoSuchElementException("pos " + i + " is not a valid index");
        }
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    /*根据属性名返回它在tdItem中的序号*/
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for(int i=0;i<tdItems.length;++i){
            if(tdItems[i].fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException("not find fieldName " + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */

    /*INT_TYPE=4，STRING_TYPE=STRING_LEN（128）+ 4 ？？？是这样理解的吗？？？*/
    //返回元祖所占的字节大小
    public int getSize() {
        // some code goes here
        int size = 0;
        for(int i=0;i<tdItems.length;++i){
            //Type中存在getLen()方法，可获取INT_TYPE和STRING_TYPE字节数。
            size += tdItems[i].fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    /*
      1.先申明一个长度为两个TupleDesc相加长度的typeAr type型以及fieldAr String型的数组
    * 2. 从两个TupleDesc中分别提取出fieldType和fieldName，
        并按照顺序一个个放入新申明的typeAr和fieldAr中
    * */
    //合并两个tditem
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] typeAr = new Type[td1.numFields() + td2.numFields()];
        String[] fieldAr = new String[td1.numFields() + td2.numFields()];
        for(int i=0;i<td1.numFields();++i){
            typeAr[i] = td1.tdItems[i].fieldType;
            fieldAr[i] = td1.tdItems[i].fieldName;
        }
        for(int i=0;i<td2.numFields();++i){
            typeAr[i+td1.numFields()] = td2.tdItems[i].fieldType;
            fieldAr[i+td1.numFields()] = td2.tdItems[i].fieldName;
        }
        return new TupleDesc(typeAr,fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal （if they have the same number of items and if the i-th type in this TupleDesc is equal to the i-th type in o * for every i.）
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    //只要两个TupleDesc的属性数量相等，且td1的第i个属性等于td2d呃第i个属性，则两个TupleDesc相等
    //指定对象与此tupledesc比较
    public boolean equals(Object o) {
        // some code goes here
        if(this.getClass().isInstance(o)) {//class.isInstance(obj) 这个对象能不能被转化为这个类，此处的意思就是o这个对象能不能被转为TupleDesc类
            TupleDesc two = (TupleDesc) o;//强转类型
            if (numFields() == two.numFields()) { //比较属性数量是否相等
                for (int i = 0; i < numFields(); ++i) { //遍历每个字段
                    //如果第i个字段类型不相等返回false
                    if (!tdItems[i].fieldType.equals(two.tdItems[i].fieldType)) {
                        return false;
                    }
                }
                return true;
            }
        }//如果遍历完了都不相等，返回false
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    //toString方法返回TupleDesc的所有属性名：“id,name,age”
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<tdItems.length-1;++i){
            sb.append(tdItems[i].fieldName + "(" + tdItems[i].fieldType + "), ");
        }
        sb.append(tdItems[tdItems.length-1].fieldName + "(" + tdItems[tdItems.length-1].fieldType + ")");
        return sb.toString();

    }
}
