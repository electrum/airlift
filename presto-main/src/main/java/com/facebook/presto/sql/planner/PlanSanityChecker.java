package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Ensures that all dependencies (i.e., symbols in expressions) for a plan node are provided by its source nodes
 */
public class PlanSanityChecker
{
    public static void validate(PlanNode plan)
    {
        plan.accept(new Visitor(), null);
    }

    private static class Visitor
        extends PlanVisitor<Void, Void>
    {
        private final Map<PlanNodeId, PlanNode> nodesById = new HashMap<>();

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getGroupBy()), "Invalid node. Group by symbols (%s) not in source plan output (%s)", node.getGroupBy(), node.getSource().getOutputSymbols());

            for (FunctionCall call : node.getAggregations().values()) {
                Set<Symbol> dependencies = DependencyExtractor.extract(call);
                Preconditions.checkArgument(source.getOutputSymbols().containsAll(dependencies), "Invalid node. Aggregation dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getPartitionBy()), "Invalid node. Partition by symbols (%s) not in source plan output (%s)", node.getPartitionBy(), node.getSource().getOutputSymbols());
            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOrderBy()), "Invalid node. Order by symbols (%s) not in source plan output (%s)", node.getOrderBy(), node.getSource().getOutputSymbols());

            for (FunctionCall call : node.getWindowFunctions().values()) {
                Set<Symbol> dependencies = DependencyExtractor.extract(call);
                Preconditions.checkArgument(source.getOutputSymbols().containsAll(dependencies), "Invalid node. Window function dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOutputSymbols()), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputSymbols(), node.getSource().getOutputSymbols());

            Set<Symbol> dependencies = DependencyExtractor.extract(node.getPredicate());

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(dependencies), "Invalid node. Predicate dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputSymbols());

            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            for (Expression expression : node.getExpressions()) {
                Set<Symbol> dependencies = DependencyExtractor.extract(expression);
                Preconditions.checkArgument(source.getOutputSymbols().containsAll(dependencies), "Invalid node. Expression dependencies (%s) not in source plan output (%s)", dependencies, node.getSource().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitTopN(TopNNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOutputSymbols()), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputSymbols(), node.getSource().getOutputSymbols());
            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOrderBy()), "Invalid node. Order by dependencies (%s) not in source plan output (%s)", node.getOrderBy(), node.getSource().getOutputSymbols());

            return null;
        }

        @Override
        public Void visitSort(SortNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOutputSymbols()), "Invalid node. Output symbols (%s) not in source plan output (%s)", node.getOutputSymbols(), node.getSource().getOutputSymbols());
            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOrderBy()), "Invalid node. Order by dependencies (%s) not in source plan output (%s)", node.getOrderBy(), node.getSource().getOutputSymbols());

            return null;
        }

        @Override
        public Void visitOutput(OutputNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            Preconditions.checkArgument(source.getOutputSymbols().containsAll(node.getOutputSymbols()), "Invalid node. Output column dependencies (%s) not in source plan output (%s)", node.getOutputSymbols(), node.getSource().getOutputSymbols());

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            verifyUniqueId(node);

            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                Preconditions.checkArgument(node.getLeft().getOutputSymbols().contains(clause.getLeft()), "Symbol from join clause (%s) not in left source (%s)", clause.getLeft(), node.getLeft().getOutputSymbols());
                Preconditions.checkArgument(node.getRight().getOutputSymbols().contains(clause.getRight()), "Symbol from join clause (%s) not in right source (%s)", clause.getRight(), node.getRight().getOutputSymbols());
            }

            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitSink(SinkNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Void context)
        {
            PlanNode source = node.getSource();
            source.accept(this, context); // visit child

            verifyUniqueId(node);

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Void context)
        {
            for (PlanNode planNode : node.getSources()) {
                Preconditions.checkArgument(planNode.getOutputSymbols().size() == node.getOutputSymbols().size(), "Each UNION query must have the same number of columns");
                planNode.accept(this, context); // visit child
            }

            verifyUniqueId(node);

            return null;
        }

        private void verifyUniqueId(PlanNode node)
        {
            PlanNodeId id = node.getId();
            Preconditions.checkArgument(!nodesById.containsKey(id), "Duplicate node id found %s between %s and %s", node.getId(), node, nodesById.get(id));

            nodesById.put(id, node);
        }
    }
}