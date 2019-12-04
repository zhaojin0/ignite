/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.type.RowType;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.internal.CU;

/** */
public class IgniteTable extends AbstractTable implements TranslatableTable, ScannableTable {
    private final String tableName;
    private final String cacheName;
    private final RowType rowType;

    public IgniteTable(String tableName, String cacheName, RowType rowType) {
        this.tableName = tableName;
        this.cacheName = cacheName;
        this.rowType = rowType;
    }

    /**
     * @return Table name;
     */
    public String tableName() {
        return tableName;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return rowType.asRelDataType(typeFactory);
    }

    /** {@inheritDoc} */
    @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        PlannerContext ctx = Commons.plannerContext(cluster.getPlanner().getContext());
        RelTraitSet traitSet = cluster.traitSet().replace(IgniteRel.IGNITE_CONVENTION)
                .replaceIf(DistributionTraitDef.INSTANCE, () -> distributionTrait(ctx));
        return new IgniteTableScan(cluster, traitSet, relOptTable);
    }

    public DistributionTrait distributionTrait(PlannerContext context) {
        return context.distributionTrait(CU.cacheId(cacheName), rowType);
    }

    public FragmentInfo fragmentInfo(PlannerContext ctx) {
        return new FragmentInfo(ctx.mapForCache(CU.cacheId(cacheName), ctx.topologyVersion()));
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
        throw new AssertionError(); // TODO
    }
}
