package simpledb.storage;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    //heapPageId由page所在的表id和page的序号组成
    private int tableId;//page所在表的id
    private int pgNo;//page的序号

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /** @return the table associated with this PageId */
    //返回该pageid所在表的id
    public int getTableId() {
        // some code goes here
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    //返回pageid对应的序号
    public int getPageNumber() {
        // some code goes here
        return pgNo;
    }

    /**
     * @return a hash code for this page, represented by a combination of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     *  @返回
     * 此页面的哈希代码，由
     * 表号和页码（如果PageId用作
     * 例如，在BufferPool中的哈希表中键入。）
     * @see BufferPool
     */
    //返回该Pageid的hashcode“表id+page序号”
    public int hashCode() {
        // some code goes here
        String hash = ""+tableId+pgNo;
        return hash.hashCode();
    }

    /**
     * Compares one PageId to another.
     * 将一个PageId与另一个PageId进行比较。
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        if (o instanceof PageId){
            PageId page = (PageId) o;
            if (page.getTableId() == tableId && page.getPageNumber() == pgNo){
                return true;
            }
        }
        return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
