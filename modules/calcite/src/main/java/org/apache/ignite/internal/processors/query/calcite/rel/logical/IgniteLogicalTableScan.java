/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.rel.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.CloneContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.splitter.SourceDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;

public final class IgniteLogicalTableScan extends TableScan implements IgniteRel {
  public IgniteLogicalTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
    super(cluster, traitSet, table);
  }

  @Override public IgniteRel clone(CloneContext ctx) {
    return new IgniteLogicalTableScan(ctx.getCluster(), getTraitSet(), getTable());
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();

    return this;
  }

  public SourceDistribution tableDistribution() {
    boolean local = !getTraitSet().isEnabled(DistributionTraitDef.INSTANCE);
    return getTable().unwrap(IgniteTable.class).sourceDistribution(getCluster().getPlanner().getContext(), local);
  }
}
