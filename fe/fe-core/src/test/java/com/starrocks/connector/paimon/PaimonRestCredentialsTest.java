// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.paimon;

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.paimon.fs.CosnFileIO;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.tencent.TencentCloudConfiguration;
import com.starrocks.thrift.TCloudConfiguration;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Map;

public class PaimonRestCredentialsTest {

    private static final String ENDPOINT = "cos-internal.ap-guangzhou.tencentcos.cn";

    @Mocked
    private RESTTokenFileIO restTokenFileIO;

    @Test
    public void testMissingInputFallsBack(@Mocked Catalog catalog) {
        Assertions.assertNull(PaimonRestCredentials.buildCloudConfiguration(null, catalog));
        Assertions.assertNull(PaimonRestCredentials.buildCloudConfiguration(stubTable("t1", stubFileIO()), null));
    }

    /** A filesystem/hive/jdbc catalog has no data token, the caller keeps its catalog level config. */
    @Test
    public void testCatalogWithoutDataTokenFallsBack(@Mocked Catalog catalog) {
        Table table = stubTable("t1", stubFileIO());
        Assertions.assertNull(PaimonRestCredentials.buildCloudConfiguration(table, catalog));
    }

    /** A token that carries no credentials (a server without data tokens) must not be forwarded. */
    @Test
    public void testTokenWithoutCredentialsFallsBack(@Mocked Catalog catalog) {
        new Expectations() {
            {
                restTokenFileIO.validToken();
                result = new RESTToken(Map.of(), System.currentTimeMillis() + 60_000L);
            }
        };
        Table table = stubTable("t1", restTokenFileIO);
        Assertions.assertNull(PaimonRestCredentials.buildCloudConfiguration(table, catalog));
    }

    /**
     * The token has to reach the backend as aws.s3.* and, importantly, every value forwarded to
     * thrift has to be non null: a null map value only blows up when the plan is serialized, which
     * is far away from here.
     */
    @Test
    public void testTokenIsForwardedWithoutNullValues(@Mocked Catalog catalog) {
        new Expectations() {
            {
                restTokenFileIO.validToken();
                result = new RESTToken(Map.of(
                        CosnFileIO.KEY_SECRET_ID, "secret-id",
                        CosnFileIO.KEY_SECRET_KEY, "secret-key",
                        CosnFileIO.KEY_SESSION_TOKEN, "session-token"), System.currentTimeMillis() + 3_600_000L);
                catalog.options();
                result = Map.of(CosnFileIO.KEY_ENDPOINT_SUFFIX, ENDPOINT);
            }
        };

        CloudConfiguration configuration =
                PaimonRestCredentials.buildCloudConfiguration(stubTable("t1", restTokenFileIO), catalog);
        Assertions.assertInstanceOf(TencentCloudConfiguration.class, configuration);

        TCloudConfiguration thrift = new TCloudConfiguration();
        configuration.toThrift(thrift);
        Map<String, String> properties = thrift.getCloud_properties();
        Assertions.assertEquals(ENDPOINT, properties.get("aws.s3.endpoint"));
        Assertions.assertEquals("secret-id", properties.get("aws.s3.access_key"));
        Assertions.assertEquals("secret-key", properties.get("aws.s3.secret_key"));
        Assertions.assertEquals("session-token", properties.get("aws.s3.session_token"));
        properties.forEach((key, value) ->
                Assertions.assertNotNull(value, "thrift cloud property " + key + " would break serialization"));
    }

    /** The same token forwarding for an S3 backed Paimon REST catalog. */
    @Test
    public void testS3TokenIsForwardedWithoutNullValues(@Mocked Catalog catalog) {
        new Expectations() {
            {
                restTokenFileIO.validToken();
                result = new RESTToken(Map.of(
                        "fs.s3a.access.key", "access-key",
                        "fs.s3a.secret.key", "secret-key",
                        "fs.s3a.session.token", "session-token"), System.currentTimeMillis() + 3_600_000L);
                catalog.options();
                result = Map.of(
                        "fs.s3a.endpoint", "cos.ap-guangzhou.myqcloud.com",
                        "fs.s3a.endpoint.region", "ap-guangzhou");
            }
        };

        CloudConfiguration configuration =
                PaimonRestCredentials.buildCloudConfiguration(stubTable("t1", restTokenFileIO), catalog);
        TCloudConfiguration thrift = new TCloudConfiguration();
        configuration.toThrift(thrift);
        Map<String, String> properties = thrift.getCloud_properties();
        Assertions.assertEquals("access-key", properties.get("aws.s3.access_key"));
        Assertions.assertEquals("secret-key", properties.get("aws.s3.secret_key"));
        Assertions.assertEquals("session-token", properties.get("aws.s3.session_token"));
        Assertions.assertEquals("cos.ap-guangzhou.myqcloud.com", properties.get("aws.s3.endpoint"));
        Assertions.assertEquals("ap-guangzhou", properties.get("aws.s3.region"));
        properties.forEach((key, value) ->
                Assertions.assertNotNull(value, "thrift cloud property " + key + " would break serialization"));
    }

    /**
     * COS needs an explicit endpoint: without one the backend would fall back to the AWS default
     * endpoint and report a misleading "bucket does not exist".
     */
    @Test
    public void testCosnTokenWithoutEndpointIsRejected(@Mocked Catalog catalog) {
        new Expectations() {
            {
                restTokenFileIO.validToken();
                result = new RESTToken(Map.of(
                        CosnFileIO.KEY_SECRET_ID, "secret-id",
                        CosnFileIO.KEY_SECRET_KEY, "secret-key"), System.currentTimeMillis() + 3_600_000L);
                catalog.options();
                result = Map.of();
            }
        };
        Table table = stubTable("t1", restTokenFileIO);
        StarRocksConnectorException e = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> PaimonRestCredentials.buildCloudConfiguration(table, catalog));
        Assertions.assertTrue(e.getMessage().contains(CosnFileIO.KEY_ENDPOINT_SUFFIX), e.getMessage());
    }

    /** Refreshing the token goes over the network; a failure must be reported, not hidden. */
    @Test
    public void testTokenFetchFailureIsReported(@Mocked Catalog catalog) {
        new Expectations() {
            {
                restTokenFileIO.validToken();
                result = new RuntimeException("connection refused");
            }
        };
        Table table = stubTable("t1", restTokenFileIO);
        StarRocksConnectorException e = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> PaimonRestCredentials.buildCloudConfiguration(table, catalog));
        Assertions.assertTrue(e.getMessage().contains("t1"), e.getMessage());
    }

    private static Table stubTable(String name, FileIO fileIO) {
        return (Table) Proxy.newProxyInstance(PaimonRestCredentialsTest.class.getClassLoader(),
                new Class<?>[] {Table.class}, (proxy, method, args) -> {
                    if ("fileIO".equals(method.getName())) {
                        return fileIO;
                    }
                    if ("name".equals(method.getName())) {
                        return name;
                    }
                    return null;
                });
    }

    private static FileIO stubFileIO() {
        return (FileIO) Proxy.newProxyInstance(PaimonRestCredentialsTest.class.getClassLoader(),
                new Class<?>[] {FileIO.class}, (proxy, method, args) -> null);
    }
}
