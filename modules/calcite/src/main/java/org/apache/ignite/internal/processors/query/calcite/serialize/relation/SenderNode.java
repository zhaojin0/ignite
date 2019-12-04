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

package org.apache.ignite.internal.processors.query.calcite.serialize.relation;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.splitter.Target;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class SenderNode extends RelGraphNode {
    private final Target target;

    private SenderNode(Target target) {
        this.target = target;
    }

    public static SenderNode create(IgniteSender rel) {
        return new SenderNode(rel.target());
    }

    @Override public RelNode toRel(ConversionContext ctx, List<RelNode> children) {
        return IgniteSender.create(F.first(children), target);
    }
}
