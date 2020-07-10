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

package org.elasticsearch.repositories.hdfs;

import io.crate.action.sql.SQLActionException;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import org.elasticsearch.plugins.Plugin;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class HdfsRepositoryIntegrationTest extends SQLTransportIntegrationTest {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(HdfsPlugin.class);
        return plugins;
    }

    @Test
    public void test_unable_to_create_repository() throws Throwable {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("[] [test] Unable verify repository, could not access blob container");
        execute("Create repository test type hdfs with (path='foo' uri='hdfs:/127.0.0.1:23/')");
    }
}
