package simpledb.optimizer;
import simpledb.optimizer.JoinOptimizer;
import simpledb.optimizer.LogicalJoinNode;

import java.util.List;

/** Class returned by {@link JoinOptimizer#} specifying the
    cost and cardinality of the optimal plan represented by plan.
*/
public class CostCard {
    /** The cost of the optimal subplan */
    public double cost; //该连接顺序下的代价
    /** The cardinality of the optimal subplan */
    public int card;//该连接顺序下的基数
    /** The optimal subplan */
    public List<LogicalJoinNode> plan;//按照某一顺序连接的查询计划
}
