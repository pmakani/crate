/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.sql.tree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

public class Update extends Statement {

    private final Relation relation;
    private final List<Assignment<Expression>> assignments;
    private final Optional<Expression> where;
    private final List<SelectItem> returning;

    public Update(Relation relation,
                  List<Assignment<Expression>> assignments,
                  Optional<Expression> where,
                  List<SelectItem> returning) {
        Preconditions.checkNotNull(relation, "relation is null");
        Preconditions.checkNotNull(assignments, "assignments are null");
        this.relation = relation;
        this.assignments = assignments;
        this.where = where;
        this.returning = returning;
    }

    public Relation relation() {
        return relation;
    }

    public List<Assignment<Expression>> assignments() {
        return assignments;
    }

    public Optional<Expression> whereClause() {
        return where;
    }

    public List<SelectItem> returningClause() {
        return returning;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(relation, assignments, where, returning);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("relation", relation)
            .add("assignments", assignments)
            .add("where", where)
            .add("returning", returning)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Update update = (Update) o;

        if (!assignments.equals(update.assignments)) return false;
        if (!relation.equals(update.relation)) return false;
        if (!java.util.Objects.equals(where, update.where)) return false;
        if (!java.util.Objects.equals(returning, update.returning)) return false;

        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUpdate(this, context);
    }

}
