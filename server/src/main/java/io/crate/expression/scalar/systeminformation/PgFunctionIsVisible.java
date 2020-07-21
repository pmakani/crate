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

package io.crate.expression.scalar.systeminformation;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import static io.crate.expression.AbstractFunctionModule.getFunctionSignatureByOid;

public final class PgFunctionIsVisible extends Scalar<Boolean, Integer> {

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                "pg_function_is_visible",
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.BOOLEAN.getTypeSignature()
            ),
            PgFunctionIsVisible::new
        );
    }

    private Signature signature;
    private Signature boundSignature;

    private PgFunctionIsVisible(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, Input<Integer>... args) {
        assert args.length == 1 : "pg_function_is_visible expects exactly 1 argument, got: " + args.length;
        Integer funcOid = args[0].value();
        return funcOid != null && getFunctionSignatureByOid(funcOid) != null;
    }
}
