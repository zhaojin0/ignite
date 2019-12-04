/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exec;

import com.google.common.collect.ImmutableList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.interpreter.Scalar;
import org.apache.calcite.interpreter.Util;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScannableTable;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.calcite.exchange.Outbox;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.splitter.Target;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunctionFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.query.calcite.prepare.ContextValue.PLANNER_CONTEXT;
import static org.apache.ignite.internal.processors.query.calcite.prepare.ContextValue.QUERY_ID;

/**
 *
 */
public class Interpretable {
    public static final Convention INTERPRETABLE = new Convention.Impl("INTERPRETABLE", InterRel.class) {};

    public static final List<RelOptRule> RULES = ImmutableList.of(
        new TableScanConverter(),
        new JoinConverter(),
        new ProjectConverter(),
        new FilterConverter(),
        new SenderConverter(),
        new ReceiverConverter()
    );

    public interface InterRel extends RelNode {
        <T> Node<T> implement(Implementor<T> implementor);
    }

    public static class TableScanConverter extends ConverterRule {
        public TableScanConverter() {
            super(IgniteTableScan.class, IgniteRel.IGNITE_CONVENTION, INTERPRETABLE, "TableScanConverter");
        }

        @Override public RelNode convert(RelNode rel) {
            IgniteTableScan scan = (IgniteTableScan) rel;

            RelTraitSet traitSet = scan.getTraitSet().replace(INTERPRETABLE);
            return new ScanRel(rel.getCluster(), traitSet, scan.getTable());
        }
    }

    public static class JoinConverter extends ConverterRule {
        public JoinConverter() {
            super(IgniteJoin.class, IgniteRel.IGNITE_CONVENTION, INTERPRETABLE, "JoinConverter");
        }

        @Override public RelNode convert(RelNode rel) {
            IgniteJoin join = (IgniteJoin) rel;

            RelNode left = convert(join.getLeft(), INTERPRETABLE);
            RelNode right = convert(join.getRight(), INTERPRETABLE);

            RelTraitSet traitSet = join.getTraitSet().replace(INTERPRETABLE);

            return new JoinRel(rel.getCluster(), traitSet, left, right, join.getCondition(), join.getVariablesSet(), join.getJoinType());
        }
    }

    public static class ProjectConverter extends ConverterRule {
        public ProjectConverter() {
            super(IgniteProject.class, IgniteRel.IGNITE_CONVENTION, INTERPRETABLE, "ProjectConverter");
        }

        @Override public RelNode convert(RelNode rel) {
            IgniteProject project = (IgniteProject) rel;
            RelTraitSet traitSet = project.getTraitSet().replace(INTERPRETABLE);
            RelNode input = convert(project.getInput(), INTERPRETABLE);

            return new ProjectRel(rel.getCluster(), traitSet, input, project.getProjects(), project.getRowType());
        }
    }

    public static class FilterConverter extends ConverterRule {
        public FilterConverter() {
            super(IgniteFilter.class, IgniteRel.IGNITE_CONVENTION, INTERPRETABLE, "FilterConverter");
        }

        @Override public RelNode convert(RelNode rel) {
            IgniteFilter filter = (IgniteFilter) rel;
            RelTraitSet traitSet = filter.getTraitSet().replace(INTERPRETABLE);
            RelNode input = convert(filter.getInput(), INTERPRETABLE);

            return new FilterRel(rel.getCluster(), traitSet, input, filter.getCondition());
        }
    }

    public static class SenderConverter extends ConverterRule {
        public SenderConverter() {
            super(IgniteSender.class, IgniteRel.IGNITE_CONVENTION, INTERPRETABLE, "SenderConverter");
        }

        @Override public RelNode convert(RelNode rel) {
            IgniteSender sender = (IgniteSender) rel;
            RelTraitSet traitSet = sender.getTraitSet().replace(INTERPRETABLE);
            RelNode input = convert(sender.getInput(), INTERPRETABLE);

            return new SenderRel(rel.getCluster(), traitSet, input, sender.target());
        }
    }

    public static class ReceiverConverter extends ConverterRule {
        public ReceiverConverter() {
            super(IgniteReceiver.class, IgniteRel.IGNITE_CONVENTION, INTERPRETABLE, "ReceiverConverter");
        }

        @Override public RelNode convert(RelNode rel) {
            IgniteReceiver sender = (IgniteReceiver) rel;
            RelTraitSet traitSet = sender.getTraitSet().replace(INTERPRETABLE);

            return new ReceiverRel(rel.getCluster(), traitSet, sender.source());
        }
    }

    public static class ReceiverRel extends AbstractRelNode implements InterRel {
        private final org.apache.ignite.internal.processors.query.calcite.splitter.Source source;

        protected ReceiverRel(RelOptCluster cluster, RelTraitSet traits, org.apache.ignite.internal.processors.query.calcite.splitter.Source source) {
            super(cluster, traits);

            this.source = source;
        }

        @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new ReceiverRel(getCluster(), traitSet, source);
        }

        @Override public <T> Node<T> implement(Implementor<T> implementor) {
            return implementor.implement(this);
        }
    }

    public static class SenderRel extends SingleRel implements InterRel {
        private final Target target;

        protected SenderRel(RelOptCluster cluster, RelTraitSet traits, RelNode input, @NotNull Target target) {
            super(cluster, traits, input);
            this.target = target;
        }

        @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new SenderRel(getCluster(), traitSet, sole(inputs), target);
        }

        @Override public <T> Node<T> implement(Implementor<T> implementor) {
            return implementor.implement(this);
        }
    }

    public static class FilterRel extends Filter implements InterRel {
        protected FilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
            super(cluster, traits, child, condition);
        }

        @Override public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
            return new FilterRel(getCluster(), traitSet, input, condition);
        }

        @Override public <T> Node<T> implement(Implementor<T> implementor) {
            return implementor.implement(this);
        }
    }

    public static class ProjectRel extends Project implements InterRel {
        protected ProjectRel(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
            super(cluster, traits, input, projects, rowType);
        }

        @Override public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
            return new ProjectRel(getCluster(), traitSet, input, projects, rowType);
        }

        @Override public <T> Node<T> implement(Implementor<T> implementor) {
            return implementor.implement(this);
        }
    }

    public static class JoinRel extends Join implements InterRel {
        protected JoinRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
            super(cluster, traitSet, left, right, condition, variablesSet, joinType);
        }

        @Override public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
            return new JoinRel(getCluster(), traitSet, left, right, condition, variablesSet, joinType);
        }

        @Override public <T> Node<T> implement(Implementor<T> implementor) {
            return implementor.implement(this);
        }
    }

    public static class ScanRel extends TableScan implements InterRel {
        protected ScanRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
            super(cluster, traitSet, table);
        }

        @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return this;
        }

        @Override public <T> Node<T> implement(Implementor<T> implementor) {
            return implementor.implement(this);
        }
    }

    public static class Implementor<T> {
        private final PlannerContext ctx;
        private final DataContext root;
        private final ScalarFactory factory;
        private Deque<Sink<T>> stack;

        public Implementor(DataContext root) {
            this.root = root;

            ctx = PLANNER_CONTEXT.get(root);
            factory = new ScalarFactory(new RexBuilder(ctx.typeFactory()));
            stack = new ArrayDeque<>();
        }

        public Node<T> implement(SenderRel rel) {
            assert stack.isEmpty();

            GridCacheVersion id = QUERY_ID.get(root);
            long exchangeId = rel.target.exchangeId();
            NodesMapping mapping = rel.target.mapping();
            List<UUID> targets = mapping.nodes();
            DistributionTrait distribution = rel.target.distribution();
            DestinationFunctionFactory destFactory = distribution.destinationFunctionFactory();
            DestinationFunction function = destFactory.create(ctx, mapping, distribution.keys());

            Outbox<T> res = new Outbox<>(id, exchangeId, targets, function);

            stack.push(res.sink());

            res.source(source(rel.getInput()));

            return res;
        }

        public Node<T> implement(FilterRel rel) {
            assert !stack.isEmpty();

            FilterNode res = new FilterNode((Sink<Object[]>) stack.pop(), factory.filterPredicate(root, rel.getCondition(), rel.getRowType()));

            stack.push((Sink<T>) res.sink());

            res.source(source(rel.getInput()));

            return (Node<T>) res;
        }

        public Node<T> implement(ProjectRel rel) {
            assert !stack.isEmpty();

            ProjectNode res = new ProjectNode((Sink<Object[]>) stack.pop(), factory.projectExpression(root, rel.getProjects(), rel.getInput().getRowType()));

            stack.push((Sink<T>) res.sink());

            res.source(source(rel.getInput()));

            return (Node<T>) res;
        }

        public Node<T> implement(JoinRel rel) {
            assert !stack.isEmpty();

            JoinNode res = new JoinNode((Sink<Object[]>) stack.pop(), factory.joinExpression(root, rel.getCondition(), rel.getLeft().getRowType(), rel.getRight().getRowType()));

            stack.push((Sink<T>) res.sink(1));
            stack.push((Sink<T>) res.sink(0));

            res.sources(sources(rel.getInputs()));

            return (Node<T>) res;
        }

        public Node<T> implement(ScanRel rel) {
            assert !stack.isEmpty();

            Iterable<Object[]> source = rel.getTable().unwrap(ScannableTable.class).scan(root);

            return (Node<T>) new ScanNode((Sink<Object[]>)stack.pop(), source);
        }

        public Node<T> implement(ReceiverRel rel) {
            throw new AssertionError(); // TODO
        }

        private Source source(RelNode rel) {
            if (rel.getConvention() != INTERPRETABLE)
                throw new IllegalStateException("INTERPRETABLE is required.");

            return ((InterRel)rel).implement(this);
        }

        private List<Source> sources(List<RelNode> rels) {
            ArrayList<Source> res = new ArrayList<>(rels.size());

            for (RelNode rel : rels) {
                res.add(source(rel));
            }

            return res;
        }

        public Node<T> go(RelNode rel) {
            if (rel.getConvention() != INTERPRETABLE)
                throw new IllegalStateException("INTERPRETABLE is required.");

            if (rel instanceof SenderRel)
                return implement((SenderRel)rel);

            ConsumerNode res = new ConsumerNode();

            stack.push((Sink<T>) res.sink());

            res.source(source(rel));

            return (Node<T>) res;
        }
    }

    /**
     *
     */
    private static class ScalarFactory {
        private final JaninoRexCompiler rexCompiler;
        private final RexBuilder builder;

        private ScalarFactory(RexBuilder builder) {
            rexCompiler = new JaninoRexCompiler(builder);
            this.builder = builder;
        }

        public <T> Predicate<T> filterPredicate(DataContext root, RexNode filter, RelDataType rowType) {
            System.out.println("filterPredicate for" + filter);

            Scalar scalar = rexCompiler.compile(ImmutableList.of(filter), rowType);
            Context ctx = Util.createContext(root);

            return new FilterPredicate<>(ctx, scalar);
        }

        public <T> Function<T, T> projectExpression(DataContext root, List<RexNode> projects, RelDataType rowType) {
            System.out.println("joinExpression for" + projects);

            Scalar scalar = rexCompiler.compile(projects, rowType);
            Context ctx = Util.createContext(root);
            int count = projects.size();

            return new ProjectExpression<>(ctx, scalar, count);
        }

        public <T> BiFunction<T, T, T> joinExpression(DataContext root, RexNode expression, RelDataType leftType, RelDataType rightType) {
            System.out.println("joinExpression for" + expression);

            RelDataType rowType = combinedType(leftType, rightType);

            Scalar scalar = rexCompiler.compile(ImmutableList.of(expression), rowType);
            Context ctx = Util.createContext(root);
            ctx.values = new Object[rowType.getFieldCount()];

            return new JoinExpression<>(ctx, scalar);
        }

        private RelDataType combinedType(RelDataType... types) {
            RelDataTypeFactory.Builder typeBuilder = new RelDataTypeFactory.Builder(builder.getTypeFactory());

            for (RelDataType type : types)
                typeBuilder.addAll(type.getFieldList());

            return typeBuilder.build();
        }

        private static class FilterPredicate<T> implements Predicate<T> {
            private final Context ctx;
            private final Scalar scalar;
            private final Object[] vals;

            private FilterPredicate(Context ctx, Scalar scalar) {
                this.ctx = ctx;
                this.scalar = scalar;

                vals = new Object[1];
            }

            @Override public boolean test(T r) {
                ctx.values = (Object[]) r;
                scalar.execute(ctx, vals);
                return (Boolean) vals[0];
            }
        }

        private static class JoinExpression<T> implements BiFunction<T, T, T> {
            private final Object[] vals;
            private final Context ctx;
            private final Scalar scalar;

            private Object[] left0;

            private JoinExpression(Context ctx, Scalar scalar) {
                this.ctx = ctx;
                this.scalar = scalar;

                vals = new Object[1];
            }

            @Override public T apply(T left, T right) {
                if (left0 != left) {
                    left0 = (Object[]) left;
                    System.arraycopy(left0, 0, ctx.values, 0, left0.length);
                }

                Object[] right0 = (Object[]) right;
                System.arraycopy(right0, 0, ctx.values, left0.length, right0.length);

                scalar.execute(ctx, vals);

                if ((Boolean) vals[0])
                    return (T) Arrays.copyOf(ctx.values, ctx.values.length);

                return null;
            }
        }

        private static class ProjectExpression<T> implements Function<T, T> {
            private final Context ctx;
            private final Scalar scalar;
            private final int count;

            private ProjectExpression(Context ctx, Scalar scalar, int count) {
                this.ctx = ctx;
                this.scalar = scalar;
                this.count = count;
            }

            @Override public T apply(T r) {
                ctx.values = (Object[]) r;
                Object[] res = new Object[count];
                scalar.execute(ctx, res);

                return (T) res;
            }
        }
    }
}
