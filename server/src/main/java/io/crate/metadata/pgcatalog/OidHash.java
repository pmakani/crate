/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.pgcatalog;

import io.crate.common.collections.Lists2;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionName;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.functions.Signature;
import io.crate.types.TypeSignature;
import org.apache.lucene.util.BytesRef;

import java.util.List;

import static org.apache.lucene.util.StringHelper.murmurhash3_x86_32;

public final class OidHash {

    enum Type {
        SCHEMA,
        TABLE,
        VIEW,
        CONSTRAINT,
        PRIMARY_KEY,
        PROC
    }

    public static int relationOid(RelationInfo relationInfo) {
        Type t = relationInfo.relationType() == RelationInfo.RelationType.VIEW ? Type.VIEW : Type.TABLE;
        BytesRef b = new BytesRef(t.toString() + relationInfo.ident().fqn());
        return murmurhash3_x86_32(b.bytes, b.offset, b.length, 0);
    }

    public static int schemaOid(String name) {
        BytesRef b = new BytesRef(Type.SCHEMA.toString() + name);
        return murmurhash3_x86_32(b.bytes, b.offset, b.length, 0);
    }

    public static int primaryKeyOid(RelationInfo relationInfo) {
        var primaryKey = Lists2.joinOn(" ", relationInfo.primaryKey(), ColumnIdent::name);
        var b = new BytesRef(Type.PRIMARY_KEY.toString() + relationInfo.ident().fqn() + primaryKey);
        return murmurhash3_x86_32(b.bytes, b.offset, b.length, 0);
    }

    static int constraintOid(String relationName, String constraintName, String constraintType) {
        BytesRef b = new BytesRef(Type.CONSTRAINT.toString() + relationName + constraintName + constraintType);
        return murmurhash3_x86_32(b.bytes, b.offset, b.length, 0);
    }

    public static int regprocOid(FunctionName name) {
        BytesRef b = new BytesRef(Type.PROC.toString() + name.schema() + name.name());
        return murmurhash3_x86_32(b.bytes, b.offset, b.length, 0);
    }

    public static int regprocOid(String name) {
        return regprocOid(new FunctionName(null, name));
    }

    public static int functionOid(Signature sig) {
        FunctionName name = sig.getName();
        BytesRef b = new BytesRef(
            new StringBuilder(Type.PROC.toString())
                .append(name.schema() == null ? "" : name.schema())
                .append(name.name())
                .append(argTypesToStr(sig.getArgumentTypes()))
                .toString());
        return murmurhash3_x86_32(b.bytes, b.offset, b.length, 0);
    }

    private static String argTypesToStr(List<TypeSignature> typeSignatures) {
        return Lists2.joinOn(" ", typeSignatures, typeSignature -> {
            try {
                return typeSignature.createType().getName();
            } catch (IllegalArgumentException i) {
                // generic signatures, e.g. E, array[E]
                String baseName = typeSignature.getBaseTypeName();
                List<TypeSignature> innerTypeSignatures = typeSignature.getParameters();
                return innerTypeSignatures.isEmpty() ?
                    "[" + baseName + "]" : baseName + argTypesToStr(innerTypeSignatures);
            }
        });
    }
}
