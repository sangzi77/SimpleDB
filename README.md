# SimpleDB
http://db.csail.mit.edu/6.830/
SimpleDB是MIT基于数据库内核教学实现的 OLTP 的关系型数据库

    Lab 1: SimpleDB 
    Lab 2: SimpleDB Operators 
    Lab 3: B+ Tree Index 
    Lab 4: SimpleDB Transactions 
    Lab 5: Query Optimization 
    Lab 6: Rollback and Recovery
    
项目简介：SimpleDB是MIT基于数据库内核教学实现的 OLTP 的关系型数据库。\
项目模块：数据库的基本存储、操作算子、淘汰策略、查询优化、事务、索引、日志回滚和恢复等。

技术要点：\
实现了基于LRU页面置换算法的 BufferPool Management。\
通过实现严格二阶段锁，实现了事务的并发控制。\
通过对表数据进行采样建立直方图并且建立 Cost Model 对 Filter 和 Join 的代价估算。\
实现了B+树作为数据库的索引，并实现了数据的查询、插入、删除、页分裂、页合并等操作。\
缓冲池使用 Steal/No-Force 策略,并通过预写式日志实现事务的恢复和回滚操作。


