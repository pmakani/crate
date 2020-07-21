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

package io.crate.expression;

import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionProvider;
import io.crate.metadata.FunctionType;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.OidHash;
import io.crate.types.DataType;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public abstract class AbstractFunctionModule<T extends FunctionImplementation> extends AbstractModule {

    private static ConcurrentMap<Integer, Signature> SYSTEM_FUNCTION_SIGNATURES_BY_OID = new ConcurrentHashMap<>();
    private static ConcurrentMap<Integer, Signature> UDF_FUNCTION_SIGNATURES_BY_OID = new ConcurrentHashMap<>();

    private HashMap<FunctionName, List<FunctionProvider>> functionImplementations = new HashMap<>();
    private MapBinder<FunctionName, List<FunctionProvider>> implementationsBinder;

    public void register(Signature signature, BiFunction<Signature, Signature, FunctionImplementation> factory) {
        List<FunctionProvider> functions = functionImplementations.computeIfAbsent(
            signature.getName(),
            k -> new ArrayList<>());
        var duplicate = functions.stream().filter(fr -> fr.getSignature().equals(signature)).findFirst();
        if (duplicate.isPresent()) {
            throw new IllegalStateException(
                "A function already exists for signature = " + signature);
        }
        functions.add(new FunctionProvider(signature, factory));
        SYSTEM_FUNCTION_SIGNATURES_BY_OID.put(OidHash.functionOid(signature), signature);
    }

    public static void registerUDFFunction(UserDefinedFunctionMetadata metadata) {
        UDF_FUNCTION_SIGNATURES_BY_OID.putIfAbsent(
            OidHash.functionOid(metadata.schema(), metadata.name(), metadata.argumentTypes()),
            Signature
                .builder()
                .kind(FunctionType.SCALAR)
                .name(new FunctionName(metadata.schema(), metadata.name()))
                .argumentTypes(metadata
                                   .argumentTypes()
                                   .stream()
                                   .map(DataType::getTypeSignature)
                                   .collect(Collectors.toList()))
                .returnType(metadata.returnType().getTypeSignature())
                .build());
    }

    public static void deregisterUDFFunction(UserDefinedFunctionMetadata metadata) {
        UDF_FUNCTION_SIGNATURES_BY_OID.remove(
            OidHash.functionOid(metadata.schema(), metadata.name(), metadata.argumentTypes()));
    }

    public static Signature getFunctionSignatureByOid(Integer functionOid) {
        if (functionOid == null) {
            new IllegalArgumentException("function oid cannot be null");
        }
        Signature signature = UDF_FUNCTION_SIGNATURES_BY_OID.get(functionOid);
        return signature == null ? SYSTEM_FUNCTION_SIGNATURES_BY_OID.get(functionOid) : signature;
    }

    public abstract void configureFunctions();

    @Override
    protected void configure() {
        configureFunctions();

        implementationsBinder = MapBinder.newMapBinder(
            binder(),
            new TypeLiteral<FunctionName>() {},
            new TypeLiteral<List<FunctionProvider>>() {});
        for (Map.Entry<FunctionName, List<FunctionProvider>> entry : functionImplementations.entrySet()) {
            implementationsBinder.addBinding(entry.getKey()).toProvider(entry::getValue);
        }

        // clear registration maps
        functionImplementations = null;
    }
}
