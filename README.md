# SimpleDB
http://db.csail.mit.edu/6.830/ \

    Lab 1: SimpleDB 
    Lab 2: SimpleDB Operators 
    Lab 3: B+ Tree Index 
    Lab 4: SimpleDB Transactions 
    Lab 5: Query Optimization 
    Lab 6: Rollback and Recovery
    
Project Introduction：SimpleDB is an OLTP relational database implemented by MIT based on database kernel teaching。\
Project module：Basic storage, operation operator, elimination strategy, query optimization, transaction, index, log rollback and recovery of database.

Technical points：\
Realized BufferPool Management based on LRU page replacement algorithm。\
The concurrency control of transactions is realized by implementing strict two-phase locking。\
Establish histogram by sampling table data and establish Cost Model to estimate the cost of Filter and Join。\
Realize the B+tree as the index of the database, and realize the operations of data query, insertion, deletion, page splitting, page merging, etc。\
The buffer pool uses the Steal/No-Force policy and realizes transaction recovery and rollback operations through pre-write logs。


