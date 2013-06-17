package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.leftGetter;
import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.rightGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;

/**
 * Removes all computation that does is not referenced transitively from the root of the plan
 * <p/>
 * E.g.,
 * <p/>
 * {@code Output[$0] -> Project[$0 := $1 + $2, $3 = $4 / $5] -> ...}
 * <p/>
 * gets rewritten as
 * <p/>
 * {@code Output[$0] -> Project[$0 := $1 + $2] -> ...}
 */
public class PruneUnreferencedOutputs
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(types), plan, ImmutableSet.<Symbol>of());
    }

    private static class Rewriter
            extends PlanNodeRewriter<Set<Symbol>>
    {
        private final Map<Symbol, Type> types;

        public Rewriter(Map<Symbol, Type> types)
        {
            this.types = types;
        }

        @Override
        public PlanNode rewriteJoin(JoinNode node, Set<Symbol> expectedOutputs, PlanRewriter<Set<Symbol>> planRewriter)
        {
            Set<Symbol> leftInputs = ImmutableSet.<Symbol>builder()
                    .addAll(expectedOutputs)
                    .addAll(Iterables.transform(node.getCriteria(), leftGetter()))
                    .build();

            Set<Symbol> rightInputs = ImmutableSet.<Symbol>builder()
                    .addAll(expectedOutputs)
                    .addAll(Iterables.transform(node.getCriteria(), rightGetter()))
                    .build();

            PlanNode left = planRewriter.rewrite(node.getLeft(), leftInputs);
            PlanNode right = planRewriter.rewrite(node.getRight(), rightInputs);

            return new JoinNode(node.getId(), left, right, node.getCriteria());
        }

        @Override
        public PlanNode rewriteAggregation(AggregationNode node, Set<Symbol> expectedOutputs, PlanRewriter<Set<Symbol>> planRewriter)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(node.getGroupBy());

            ImmutableMap.Builder<Symbol, FunctionHandle> functions = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, FunctionCall> functionCalls = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                if (expectedOutputs.contains(symbol)) {
                    FunctionCall call = entry.getValue();
                    expectedInputs.addAll(DependencyExtractor.extract(call));

                    functionCalls.put(symbol, call);
                    functions.put(symbol, node.getFunctions().get(symbol));
                }
            }

            PlanNode source = planRewriter.rewrite(node.getSource(), expectedInputs.build());

            return new AggregationNode(node.getId(), source, node.getGroupBy(), functionCalls.build(), functions.build());
        }

        @Override
        public PlanNode rewriteWindow(WindowNode node, Set<Symbol> expectedOutputs, PlanRewriter<Set<Symbol>> planRewriter)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(expectedOutputs)
                    .addAll(node.getPartitionBy())
                    .addAll(node.getOrderBy());

            ImmutableMap.Builder<Symbol, FunctionHandle> functions = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, FunctionCall> functionCalls = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getWindowFunctions().entrySet()) {
                Symbol symbol = entry.getKey();

                if (expectedOutputs.contains(symbol)) {
                    FunctionCall call = entry.getValue();
                    expectedInputs.addAll(DependencyExtractor.extract(call));

                    functionCalls.put(symbol, call);
                    functions.put(symbol, node.getFunctionHandles().get(symbol));
                }
            }

            PlanNode source = planRewriter.rewrite(node.getSource(), expectedInputs.build());

            return new WindowNode(node.getId(), source, node.getPartitionBy(), node.getOrderBy(), node.getOrderings(), functionCalls.build(), functions.build());
        }

        @Override
        public PlanNode rewriteTableScan(TableScanNode node, Set<Symbol> expectedOutputs, PlanRewriter<Set<Symbol>> planRewriter)
        {
            boolean empty = true;

            ImmutableMap.Builder<Symbol, ColumnHandle> assignments = ImmutableMap.builder();
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                Symbol symbol = entry.getKey();

                if (expectedOutputs.contains(symbol) || expectedOutputs.isEmpty() && Type.isNumeric(types.get(symbol))) { // heuristic: numeric fields are cheaper if we have to choose an arbitrary one
                    assignments.put(symbol, entry.getValue());
                    empty = false;

                    if (expectedOutputs.isEmpty()) {
                        break;
                    }
                }
            }

            if (empty) {
                Map.Entry<Symbol, ColumnHandle> first = Iterables.getFirst(node.getAssignments().entrySet(), null);
                assignments.put(first.getKey(), first.getValue());
            }

            return new TableScanNode(node.getId(), node.getTable(), assignments.build());
        }
        @Override
        public PlanNode rewriteFilter(FilterNode node, Set<Symbol> expectedOutputs, PlanRewriter<Set<Symbol>> planRewriter)
        {
            Set<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(DependencyExtractor.extract(node.getPredicate()))
                    .addAll(expectedOutputs)
                    .build();

            PlanNode source = planRewriter.rewrite(node.getSource(), expectedInputs);

            return new FilterNode(node.getId(), source, node.getPredicate());
        }

        @Override
        public PlanNode rewriteProject(ProjectNode node, Set<Symbol> expectedOutputs, PlanRewriter<Set<Symbol>> planRewriter)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.builder();

            ImmutableMap.Builder<Symbol, Expression> builder = ImmutableMap.builder();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                Symbol output = node.getOutputSymbols().get(i);
                Expression expression = node.getExpressions().get(i);

                if (expectedOutputs.contains(output)) {
                    expectedInputs.addAll(DependencyExtractor.extract(expression));
                    builder.put(output, expression);
                }
            }

            PlanNode source = planRewriter.rewrite(node.getSource(), expectedInputs.build());

            return new ProjectNode(node.getId(), source, builder.build());
        }

        @Override
        public PlanNode rewriteOutput(OutputNode node, Set<Symbol> expectedOutputs, PlanRewriter<Set<Symbol>> planRewriter)
        {
            Set<Symbol> expectedInputs = ImmutableSet.copyOf(node.getOutputSymbols());
            PlanNode source = planRewriter.rewrite(node.getSource(), expectedInputs);
            return new OutputNode(node.getId(), source, node.getColumnNames(), node.getOutputSymbols());
        }

        @Override
        public PlanNode rewriteLimit(LimitNode node, Set<Symbol> expectedOutputs, PlanRewriter<Set<Symbol>> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), expectedOutputs);
            return new LimitNode(node.getId(), source, node.getCount());
        }

        @Override
        public PlanNode rewriteTopN(TopNNode node, Set<Symbol> expectedOutputs, PlanRewriter<Set<Symbol>> planRewriter)
        {
            Set<Symbol> expectedInputs = ImmutableSet.copyOf(concat(expectedOutputs, node.getOrderBy()));

            PlanNode source = planRewriter.rewrite(node.getSource(), expectedInputs);

            return new TopNNode(node.getId(), source, node.getCount(), node.getOrderBy(), node.getOrderings());
        }

        @Override
        public PlanNode rewriteSort(SortNode node, Set<Symbol> expectedOutputs, PlanRewriter<Set<Symbol>> planRewriter)
        {
            Set<Symbol> expectedInputs = ImmutableSet.copyOf(concat(expectedOutputs, node.getOrderBy()));

            PlanNode source = planRewriter.rewrite(node.getSource(), expectedInputs);

            return new SortNode(node.getId(), source, node.getOrderBy(), node.getOrderings());
        }

        @Override
        public PlanNode rewriteTableWriter(TableWriterNode node, Set<Symbol> expectedOutputs, PlanRewriter<Set<Symbol>> planRewriter)
        {
            // Rewrite Query subtree in terms of the symbols expected by the writer.
            Set<Symbol> expectedInputs = ImmutableSet.copyOf(node.getColumns().keySet());
            PlanNode source = planRewriter.rewrite(node.getSource(), expectedInputs);
            return new TableWriterNode(node.getId(),
                    source,
                    node.getTable(),
                    node.getColumns(),
                    node.getOutput());
        }

        @Override
        public PlanNode rewriteUnion(UnionNode node, Set<Symbol> expectedOutputs, PlanRewriter<Set<Symbol>> planRewriter)
        {
            // Find out which output symbols we need to keep
            ImmutableListMultimap.Builder<Symbol, Symbol> rewrittenSymbolMappingBuilder = ImmutableListMultimap.builder();
            for (Symbol symbol : node.getOutputSymbols()) {
                if (expectedOutputs.contains(symbol)) {
                    rewrittenSymbolMappingBuilder.putAll(symbol, node.getSymbolMapping().get(symbol));
                }
            }
            ListMultimap<Symbol, Symbol> rewrittenSymbolMapping = rewrittenSymbolMappingBuilder.build();

            // Find the corresponding input symbol to the remaining output symbols and prune the subplans
            ImmutableList.Builder<PlanNode> rewrittenSubPlans = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                ImmutableSet.Builder<Symbol> expectedInputSymbols = ImmutableSet.builder();
                for (Collection<Symbol> symbols : rewrittenSymbolMapping.asMap().values()) {
                    expectedInputSymbols.add(Iterables.get(symbols, i));
                }
                rewrittenSubPlans.add(planRewriter.rewrite(node.getSources().get(i), expectedInputSymbols.build()));
            }

            return new UnionNode(node.getId(), rewrittenSubPlans.build(), rewrittenSymbolMapping);
        }
    }
}