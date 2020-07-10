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

package org.elasticsearch.repositories.s3;

import com.amazonaws.services.s3.AbstractAmazonS3;
import io.crate.action.sql.SQLActionException;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collection;

import static org.mockito.Mockito.mock;


public class S3RepositoryIntegrationTest extends SQLTransportIntegrationTest {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(S3RepositoryPlugin.class);
        return plugins;
    }

    @Test
    public void test_unable_to_create_s3_repository() throws Throwable {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("[] [test123] Unable verify repository, could not access blob container");
        execute(
            "create repository test123 type s3 with (bucket='bucket', endpoint='https://s3.region.amazonaws.com', " +
            "protocol='https', access_key='access',secret_key='secret'\n" +
            ",base_path='test123')");
    }
}
