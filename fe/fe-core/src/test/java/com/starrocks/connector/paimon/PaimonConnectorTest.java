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

import com.google.common.collect.Lists;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonConnectorTest {

    @Test
    public void testCreatePaimonConnector() {
        Map<String, String> properties = new HashMap<>();

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties)),
                "The property paimon.catalog.type must be set.");

        properties.put("paimon.catalog.type", "filesystem");

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties)),
                "The property paimon.catalog.warehouse must be set.");
        properties.put("paimon.catalog.warehouse", "hdfs://127.0.0.1:9999/warehouse");

        new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
    }

    @Test
    public void testCreateRestPaimonConnector() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PaimonConnector.PAIMON_CATALOG_TYPE, "rest");

        // the endpoint is the only property a REST catalog needs; its warehouse comes from
        // GET /v1/config, so it must not be required here
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties)),
                "The property paimon.catalog.uri must be set if paimon catalog is rest.");

        properties.put(PaimonConnector.PAIMON_CATALOG_URI, "http://127.0.0.1:18080");
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Options options = connector.getPaimonOptions();
        Assertions.assertEquals("rest", options.get(CatalogOptions.METASTORE));
        Assertions.assertEquals("http://127.0.0.1:18080", options.get(CatalogOptions.URI));
        // without a configured token no auth option is injected, the server may not need one
        Assertions.assertNull(options.get(RESTCatalogOptions.TOKEN));
        Assertions.assertNull(options.get(RESTCatalogOptions.TOKEN_PROVIDER));
    }

    @Test
    public void testRestPaimonConnectorTokenOptions() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PaimonConnector.PAIMON_CATALOG_TYPE, "rest");
        properties.put(PaimonConnector.PAIMON_CATALOG_URI, "http://127.0.0.1:18080");
        properties.put(PaimonConnector.PAIMON_CATALOG_TOKEN, "init_token");

        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Options options = connector.getPaimonOptions();
        Assertions.assertEquals("init_token", options.get(RESTCatalogOptions.TOKEN));
        // the bearer provider is what a Paimon REST catalog server expects by default
        Assertions.assertEquals("bear", options.get(RESTCatalogOptions.TOKEN_PROVIDER));

        properties.put(PaimonConnector.PAIMON_CATALOG_TOKEN_PROVIDER, "dlf");
        PaimonConnector custom = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Assertions.assertEquals("dlf", custom.getPaimonOptions().get(RESTCatalogOptions.TOKEN_PROVIDER));
    }

    @Test
    public void testRestPaimonConnectorWarehouseIsOptional() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PaimonConnector.PAIMON_CATALOG_TYPE, "rest");
        properties.put(PaimonConnector.PAIMON_CATALOG_URI, "http://127.0.0.1:18080");
        properties.put("paimon.option.warehouse", "cosn://bucket/paimon");

        // a user supplied warehouse still wins over the one advertised by the server
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Assertions.assertEquals("cosn://bucket/paimon", connector.getPaimonOptions().get(CatalogOptions.WAREHOUSE));
    }

    @Test
    public void testRestPaimonConnectorValidatesUri() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PaimonConnector.PAIMON_CATALOG_TYPE, "rest");
        // a bare host is a common typo and would otherwise fail deep inside the paimon client
        properties.put(PaimonConnector.PAIMON_CATALOG_URI, "127.0.0.1:18080");
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties)));

        // surrounding whitespace is tolerated
        properties.put(PaimonConnector.PAIMON_CATALOG_URI, " http://127.0.0.1:18080 ");
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Assertions.assertEquals("http://127.0.0.1:18080", connector.getPaimonOptions().get(CatalogOptions.URI));
    }

    @Test
    public void testRestPaimonConnectorValidatesTokenProvider() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PaimonConnector.PAIMON_CATALOG_TYPE, "rest");
        properties.put(PaimonConnector.PAIMON_CATALOG_URI, "http://127.0.0.1:18080");
        properties.put(PaimonConnector.PAIMON_CATALOG_TOKEN, "init_token");
        properties.put(PaimonConnector.PAIMON_CATALOG_TOKEN_PROVIDER, "oauth2");
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties)));

        // the provider is normalised before it is handed to paimon
        properties.put(PaimonConnector.PAIMON_CATALOG_TOKEN_PROVIDER, " DLF ");
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Assertions.assertEquals("dlf", connector.getPaimonOptions().get(RESTCatalogOptions.TOKEN_PROVIDER));
    }

    @Test
    public void testCacheLifetimeMatchesIceberg() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.type", "filesystem");
        properties.put("paimon.catalog.warehouse", "hdfs://127.0.0.1:9999/warehouse");
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Options options = connector.getPaimonOptions();
        // 24h to match iceberg_meta_cache_ttl_sec; both must be set or a default becomes binding
        Assertions.assertEquals(Duration.ofHours(24), options.get(CatalogOptions.CACHE_EXPIRE_AFTER_ACCESS));
        Assertions.assertEquals(Duration.ofHours(24), options.get(CatalogOptions.CACHE_EXPIRE_AFTER_WRITE));
    }

    @Test
    public void testMetaCacheTtlProperty() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.type", "filesystem");
        properties.put("paimon.catalog.warehouse", "hdfs://127.0.0.1:9999/warehouse");
        properties.put(PaimonConnector.PAIMON_META_CACHE_TTL, "3600");
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Options options = connector.getPaimonOptions();
        Assertions.assertEquals(Duration.ofHours(1), options.get(CatalogOptions.CACHE_EXPIRE_AFTER_ACCESS));
        Assertions.assertEquals(Duration.ofHours(1), options.get(CatalogOptions.CACHE_EXPIRE_AFTER_WRITE));
    }

    @Test
    public void testCacheOptionsCanBeOverridden() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.type", "filesystem");
        properties.put("paimon.catalog.warehouse", "hdfs://127.0.0.1:9999/warehouse");
        properties.put("paimon.option.cache.expire-after-write", "1h");
        properties.put("paimon.option.cache.partition.max-num", "50");
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Options options = connector.getPaimonOptions();
        // the paimon.option. passthrough runs after our defaults
        Assertions.assertEquals(Duration.ofHours(1), options.get(CatalogOptions.CACHE_EXPIRE_AFTER_WRITE));
        Assertions.assertEquals(50L, options.get(CatalogOptions.CACHE_PARTITION_MAX_NUM));
        Assertions.assertEquals(Duration.ofHours(24), options.get(CatalogOptions.CACHE_EXPIRE_AFTER_ACCESS));
    }

    @Test
    public void testTableCacheRefreshIntervalProperty() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.type", "filesystem");
        properties.put("paimon.catalog.warehouse", "hdfs://127.0.0.1:9999/warehouse");
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        // mirrors iceberg_table_cache_refresh_interval_sec
        Assertions.assertEquals(60L, connector.getTableCacheRefreshIntervalSec());

        properties.put(PaimonConnector.PAIMON_TABLE_CACHE_REFRESH_INTERVAL, "0");
        PaimonConnector off = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Assertions.assertEquals(0L, off.getTableCacheRefreshIntervalSec());
    }

    @Test
    public void testCacheCanBeDisabled() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.type", "filesystem");
        properties.put("paimon.catalog.warehouse", Files.createTempDirectory("paimon_no_cache").toString());
        properties.put("paimon.option.cache-enabled", "false");
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));

        Assertions.assertFalse(connector.getPaimonOptions().get(CatalogOptions.CACHE_ENABLED));
        Assertions.assertFalse(connector.getPaimonNativeCatalog() instanceof CachingCatalog);
    }

    @Test
    public void testCreateHivePaimonConnectorWithoutUris() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.type", "hive");
        properties.put("paimon.catalog.warehouse", "hdfs://127.0.0.1:9999/warehouse");

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties)),
                "The property hive.metastore.uris must be set if paimon catalog is hive.");

        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");

        new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
    }

    @Test
    public void testCreateDLFPaimonConnector() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.type", "dlf");
        properties.put("dlf.catalog.id", "dlf_test");

        new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
    }

    @Test
    public void testCreatePaimonTable(@Mocked Catalog paimonNativeCatalog,
                                      @Mocked FileStoreTable paimonNativeTable) throws Catalog.TableNotExistException {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.warehouse", "hdfs://127.0.0.1:9999/warehouse");
        properties.put("paimon.catalog.type", "filesystem");
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(1, "col2", new IntType()));
        new Expectations(connector) {
            {
                connector.getPaimonNativeCatalog();
                result = paimonNativeCatalog;
                paimonNativeCatalog.getTable((Identifier) any);
                result = paimonNativeTable;
                paimonNativeTable.rowType().getFields();
                result = fields;
                paimonNativeTable.location().toString();
                result = "hdfs://127.0.0.1:10000/paimon";
                paimonNativeTable.partitionKeys();
                result = new ArrayList<>(Collections.singleton("col1"));
            }
        };

        ConnectorMetadata metadata = connector.getMetadata();
        Assertions.assertTrue(metadata instanceof PaimonMetadata);
        com.starrocks.catalog.Table table = metadata.getTable(new ConnectContext(), "db1", "tbl1");
        PaimonTable paimonTable = (PaimonTable) table;
        Assertions.assertEquals("db1", paimonTable.getCatalogDBName());
        Assertions.assertEquals("tbl1", paimonTable.getCatalogTableName());
        Assertions.assertEquals(Lists.newArrayList("col1"), paimonTable.getPartitionColumnNames());
        Assertions.assertEquals("hdfs://127.0.0.1:10000/paimon", paimonTable.getTableLocation());
        Assertions.assertEquals(IntegerType.INT, paimonTable.getBaseSchema().get(0).getType());
        Assertions.assertEquals("paimon_catalog", paimonTable.getCatalogName());
    }

    @Test
    public void testCreatePaimonConnectorWithS3() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.warehouse", "s3://bucket/warehouse");
        properties.put("paimon.catalog.type", "filesystem");
        String accessKeyValue = "s3_access_key";
        String secretKeyValue = "s3_secret_key";
        String endpointValue = "s3_endpoint";
        properties.put("aws.s3.access_key", accessKeyValue);
        properties.put("aws.s3.secret_key", secretKeyValue);
        properties.put("aws.s3.endpoint", endpointValue);
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Options paimonOptions = connector.getPaimonOptions();
        String accessKeyOption = paimonOptions.get("s3.access-key");
        String secretKeyOption = paimonOptions.get("s3.secret-key");
        String endpointOption = paimonOptions.get("s3.endpoint");
        Assertions.assertEquals(accessKeyOption, accessKeyValue);
        Assertions.assertEquals(secretKeyOption, secretKeyValue);
        Assertions.assertEquals(endpointOption, endpointValue);
    }

    @Test
    public void testCreatePaimonConnectorWithOSS() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.warehouse", "oss://bucket/warehouse");
        properties.put("paimon.catalog.type", "filesystem");
        String accessKeyValue = "oss_access_key";
        String secretKeyValue = "oss_secret_key";
        String endpointValue = "oss_endpoint";
        properties.put("aliyun.oss.access_key", accessKeyValue);
        properties.put("aliyun.oss.secret_key", secretKeyValue);
        properties.put("aliyun.oss.endpoint", endpointValue);
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Options paimonOptions = connector.getPaimonOptions();
        String accessKeyOption = paimonOptions.get("fs.oss.accessKeyId");
        String secretKeyOption = paimonOptions.get("fs.oss.accessKeySecret");
        String endpointOption = paimonOptions.get("fs.oss.endpoint");
        Assertions.assertEquals(accessKeyOption, accessKeyValue);
        Assertions.assertEquals(secretKeyOption, secretKeyValue);
        Assertions.assertEquals(endpointOption, endpointValue);
    }
}
