package simpledb.storage;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;

import java.io.IOException;
import java.util.*;

/**
 * DbFileIterator is the iterator interface that all SimpleDB Dbfile should
 * implement.
 */
public interface DbFileIterator{
    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    void open()
            throws DbException, TransactionAbortedException, IOException;

    /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
    boolean hasNext()
            throws DbException, TransactionAbortedException, IOException;

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    Tuple next()
            throws DbException, TransactionAbortedException, NoSuchElementException, IOException;

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    void rewind() throws DbException, TransactionAbortedException, IOException;

    /**
     * Closes the iterator.
     */
    void close();
}
