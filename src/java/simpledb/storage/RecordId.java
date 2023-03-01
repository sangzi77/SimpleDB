package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    //元组Id由pageId和元组序号构成
    private PageId pid; //页面ID(也就是找到表) //元组id所在页的pageid
    private int tupleno; // 行序号 //元组的序号
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    //返回元组序号
    public int getTupleNumber() {
        // some code goes here
        return tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    //返回元组所属页的pageid
    public PageId getPageId() {
        // some code goes here
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (o instanceof RecordId){
            RecordId recordId = (RecordId) o;
            if (pid.equals(recordId.getPageId()) && tupleno == recordId.getTupleNumber()){
                return true;
            }
        }
       return false;
    }

    /**
     * You should implement the hashCode() so that two
     * equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * @return An int that is the same for equal RecordId objects.
     */
    //hashcode方法，返回"tableId+pageNo+tupleNo"
    @Override
    public int hashCode() {
        // some code goes here
        String hash = " "+pid.getTableId()+pid.getPageNumber()+tupleno;
        return hash.hashCode();
    }

}
