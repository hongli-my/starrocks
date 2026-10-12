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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.concurrent.lock.BlockingCallValidator;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.credential.aliyun.AliyunCloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudCredential;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import com.starrocks.credential.aws.AwsCloudCredential;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.privilege.PrivilegedCatalog;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTCatalogOptions;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.URI;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;

public class PaimonConnector implements Connector {
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";
    public static final String PAIMON_CATALOG_WAREHOUSE = "paimon.catalog.warehouse";
    public static final String PAIMON_CATALOG_URI = "paimon.catalog.uri";
    public static final String PAIMON_CATALOG_TOKEN = "paimon.catalog.token";
    public static final String PAIMON_CATALOG_TOKEN_HEADER = "paimon.catalog.token.header";
    public static final String PAIMON_CATALOG_TOKEN_PROVIDER = "paimon.catalog.token.provider";
    public static final String PAIMON_REST_CATALOG_TYPE = "rest";
    private static final String PAIMON_REST_DEFAULT_TOKEN_PROVIDER = "bear";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    /** An HTTP header name is an RFC 7230 token. */
    private static final Pattern HEADER_NAME_PATTERN = Pattern.compile("^[A-Za-z0-9!#$%&'*+.^_`|~-]+$");
    // the token providers paimon ships; anything else would fail deeper inside the client
    private static final Set<String> PAIMON_REST_TOKEN_PROVIDERS = Set.of("bear", "dlf");
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private static final String DLF_CATGALOG_ID = "dlf.catalog.id";
    // implicit for user, mirrors iceberg_meta_cache_ttl_sec
    public static final String PAIMON_META_CACHE_TTL = "paimon_meta_cache_ttl_sec";
    private static final long DEFAULT_META_CACHE_TTL_SEC = 24L * 60 * 60;
    // mirrors iceberg_table_cache_refresh_interval_sec, which feeds Caffeine's refreshAfterWrite
    public static final String PAIMON_TABLE_CACHE_REFRESH_INTERVAL =
            "paimon_table_cache_refresh_interval_sec";
    private static final long DEFAULT_TABLE_CACHE_REFRESH_INTERVAL_SEC = 60L;
    private static final long CACHE_PARTITION_MAX_NUM = 1000L;
    // same sizing as the iceberg background pool
    private static final int REFRESH_THREAD_NUM = Math.max(2, Runtime.getRuntime().availableProcessors() / 8);
    private static final MemorySize CACHE_MANIFEST_FILE_THRESHOLD = MemorySize.ofMebiBytes(10);
    private static final MemorySize CACHE_MANIFEST_MEMORY = MemorySize.ofMebiBytes(1024);
    private final HdfsEnvironment hdfsEnvironment;
    private Catalog paimonNativeCatalog;
    private final ExecutorService refreshExecutor;
    private final long tableCacheRefreshIntervalSec;
    private final String catalogName;
    private final Options paimonOptions;
    private final ConnectorProperties connectorProperties;

    public PaimonConnector(ConnectorContext context) {
        Map<String, String> properties = context.getProperties();
        this.connectorProperties = new ConnectorProperties(ConnectorType.PAIMON, properties);
        this.catalogName = context.getCatalogName();
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
        String catalogType = properties.get(PAIMON_CATALOG_TYPE);
        String metastoreUris = properties.get(HIVE_METASTORE_URIS);
        String warehousePath = properties.get(PAIMON_CATALOG_WAREHOUSE);

        this.paimonOptions = new Options();
        if (Strings.isNullOrEmpty(catalogType)) {
            throw new StarRocksConnectorException("The property %s must be set.", PAIMON_CATALOG_TYPE);
        }
        paimonOptions.setString(METASTORE.key(), catalogType);
        if (catalogType.equals("hive")) {
            if (!Strings.isNullOrEmpty(metastoreUris)) {
                paimonOptions.setString(URI.key(), metastoreUris);
            } else {
                throw new StarRocksConnectorException("The property %s must be set if paimon catalog is hive.",
                        HIVE_METASTORE_URIS);
            }
        } else if (catalogType.equalsIgnoreCase("dlf")) {
            String dlfCatalogId = properties.get(DLF_CATGALOG_ID);
            if (null != dlfCatalogId && !dlfCatalogId.isEmpty()) {
                paimonOptions.setString(DLF_CATGALOG_ID, dlfCatalogId);
            }
        } else if (catalogType.equalsIgnoreCase(PAIMON_REST_CATALOG_TYPE)) {
            // A REST catalog knows its own warehouse (GET /v1/config), so only the endpoint and,
            // when the server requires one, the bearer token have to be configured here.
            String restUri = properties.get(PAIMON_CATALOG_URI);
            if (Strings.isNullOrEmpty(restUri)) {
                throw new StarRocksConnectorException("The property %s must be set if paimon catalog is rest.",
                        PAIMON_CATALOG_URI);
            }
            restUri = restUri.trim();
            if (!restUri.startsWith("http://") && !restUri.startsWith("https://")) {
                throw new StarRocksConnectorException("The property %s must be an http(s) URL, but got '%s'.",
                        PAIMON_CATALOG_URI, restUri);
            }
            paimonOptions.setString(URI.key(), restUri);
            String token = properties.get(PAIMON_CATALOG_TOKEN);
            if (!Strings.isNullOrEmpty(token)) {
                String tokenHeader = properties
                        .getOrDefault(PAIMON_CATALOG_TOKEN_HEADER, AUTHORIZATION_HEADER).trim();
                if (!HEADER_NAME_PATTERN.matcher(tokenHeader).matches()) {
                    throw new StarRocksConnectorException("The property %s must be a valid HTTP header name, "
                            + "but got '%s'.", PAIMON_CATALOG_TOKEN_HEADER, tokenHeader);
                }
                if (AUTHORIZATION_HEADER.equalsIgnoreCase(tokenHeader)) {
                    String tokenProvider = properties
                            .getOrDefault(PAIMON_CATALOG_TOKEN_PROVIDER, PAIMON_REST_DEFAULT_TOKEN_PROVIDER)
                            .trim().toLowerCase(Locale.ROOT);
                    if (!PAIMON_REST_TOKEN_PROVIDERS.contains(tokenProvider)) {
                        throw new StarRocksConnectorException("The property %s must be one of %s, but got '%s'.",
                                PAIMON_CATALOG_TOKEN_PROVIDER, PAIMON_REST_TOKEN_PROVIDERS, tokenProvider);
                    }
                    paimonOptions.setString(RESTCatalogOptions.TOKEN.key(), token);
                    paimonOptions.setString(RESTCatalogOptions.TOKEN_PROVIDER.key(), tokenProvider);
                } else {
                    if (properties.containsKey(PAIMON_CATALOG_TOKEN_PROVIDER)) {
                        throw new StarRocksConnectorException("The property %s only applies when %s is '%s'.",
                                PAIMON_CATALOG_TOKEN_PROVIDER, PAIMON_CATALOG_TOKEN_HEADER, AUTHORIZATION_HEADER);
                    }
                    // The token travels verbatim on the server's own header, through the
                    // header.<name> option paimon puts on every request. Paimon still insists on a
                    // token provider being set, so point it at the one that adds no Authorization
                    // header of its own.
                    paimonOptions.setString(RESTApi.HEADER_PREFIX + tokenHeader, token);
                    paimonOptions.setString(RESTCatalogOptions.TOKEN_PROVIDER.key(),
                            HeaderTokenAuthProviderFactory.IDENTIFIER);
                }
            }
        }
        // hive / dlf / rest resolve their warehouse on their own.
        if (Strings.isNullOrEmpty(warehousePath)
                && !catalogType.equals("hive")
                && !catalogType.equalsIgnoreCase("dlf")
                && !catalogType.equalsIgnoreCase(PAIMON_REST_CATALOG_TYPE)) {
            throw new StarRocksConnectorException("The property %s must be set.", PAIMON_CATALOG_WAREHOUSE);
        }
        if (!Strings.isNullOrEmpty(warehousePath)) {
            paimonOptions.setString(WAREHOUSE.key(), warehousePath);
        }
        initFsOption(cloudConfiguration);

        // Both must be set, or the one left out keeps its default (10min / 30min) and binds instead.
        // With equal values expire-after-write binds, which is Iceberg's write-only TTL semantics.
        Duration metaCacheTtl = Duration.ofSeconds(
                PropertyUtil.propertyAsLong(properties, PAIMON_META_CACHE_TTL, DEFAULT_META_CACHE_TTL_SEC));
        this.tableCacheRefreshIntervalSec = PropertyUtil.propertyAsLong(properties, PAIMON_TABLE_CACHE_REFRESH_INTERVAL,
                DEFAULT_TABLE_CACHE_REFRESH_INTERVAL_SEC);
        // built here, not on the lazy catalog path: two concurrent first queries would otherwise
        // each build a pool and only the last one stored could ever be shut down
        this.refreshExecutor = ThreadPoolManager.newDaemonFixedThreadPoolWithUnboundedQueue(
                REFRESH_THREAD_NUM, catalogName + "-paimon-refresh-pool", true);
        this.paimonOptions.set(CatalogOptions.CACHE_EXPIRE_AFTER_ACCESS, metaCacheTtl);
        this.paimonOptions.set(CatalogOptions.CACHE_EXPIRE_AFTER_WRITE, metaCacheTtl);
        // max num of cached partitions of a Paimon catalog
        this.paimonOptions.set(CatalogOptions.CACHE_PARTITION_MAX_NUM, CACHE_PARTITION_MAX_NUM);
        // max size of cached manifest files, 10m means cache all since files usually no more than 8m
        this.paimonOptions.set(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_THRESHOLD, CACHE_MANIFEST_FILE_THRESHOLD);
        // max size of memory manifest cache uses
        this.paimonOptions.set(CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY, CACHE_MANIFEST_MEMORY);

        String keyPrefix = "paimon.option.";
        Set<String> optionKeys = properties.keySet().stream().filter(k -> k.startsWith(keyPrefix)).collect(Collectors.toSet());
        for (String k : optionKeys) {
            String key = k.substring(keyPrefix.length());
            paimonOptions.setString(key, properties.get(k));
        }
    }

    public void initFsOption(CloudConfiguration cloudConfiguration) {
        if (cloudConfiguration.getCloudType() == CloudType.AWS) {
            AwsCloudConfiguration awsCloudConfiguration = (AwsCloudConfiguration) cloudConfiguration;
            paimonOptions.set("s3.connection.ssl.enabled", String.valueOf(awsCloudConfiguration.getEnableSSL()));
            paimonOptions.set("s3.path.style.access", String.valueOf(awsCloudConfiguration.getEnablePathStyleAccess()));
            AwsCloudCredential awsCloudCredential = awsCloudConfiguration.getAwsCloudCredential();
            if (!awsCloudCredential.getEndpoint().isEmpty()) {
                paimonOptions.set("s3.endpoint", awsCloudCredential.getEndpoint());
            }
            if (!awsCloudCredential.getAccessKey().isEmpty()) {
                paimonOptions.set("s3.access-key", awsCloudCredential.getAccessKey());
            }
            if (!awsCloudCredential.getSecretKey().isEmpty()) {
                paimonOptions.set("s3.secret-key", awsCloudCredential.getSecretKey());
            }
        }
        if (cloudConfiguration.getCloudType() == CloudType.ALIYUN) {
            AliyunCloudConfiguration aliyunCloudConfiguration = (AliyunCloudConfiguration) cloudConfiguration;
            AliyunCloudCredential aliyunCloudCredential = aliyunCloudConfiguration.getAliyunCloudCredential();
            if (!aliyunCloudCredential.getEndpoint().isEmpty()) {
                paimonOptions.set("fs.oss.endpoint", aliyunCloudCredential.getEndpoint());
            }
            if (!aliyunCloudCredential.getAccessKey().isEmpty()) {
                paimonOptions.set("fs.oss.accessKeyId", aliyunCloudCredential.getAccessKey());
            }
            if (!aliyunCloudCredential.getSecretKey().isEmpty()) {
                paimonOptions.set("fs.oss.accessKeySecret", aliyunCloudCredential.getSecretKey());
            }
        }
    }

    @VisibleForTesting
    public long getTableCacheRefreshIntervalSec() {
        return tableCacheRefreshIntervalSec;
    }

    public Options getPaimonOptions() {
        return this.paimonOptions;
    }

    public Catalog getPaimonNativeCatalog() {
        if (paimonNativeCatalog == null) {
            // Inside the null check for the same reason as iceberg: a hit returns a field, only
            // the build contacts the metastore or the warehouse path's file system.
            BlockingCallValidator.validateNotUnderLock("paimon", catalogName);
            Configuration configuration = new Configuration();
            hdfsEnvironment.getCloudConfiguration().applyToConfiguration(configuration);
            CatalogContext context = CatalogContext.create(getPaimonOptions(), configuration);
            // Build the cache layer ourselves so background refresh can track access time and
            // snapshot/schema revisions; privilege wrapper stays outside, as in createCatalog.
            Catalog unwrapped = CatalogFactory.createUnwrappedCatalog(context,
                    CatalogFactory.class.getClassLoader());
            // Directly around the unwrapped catalog, so the per-call door only sees requests that
            // are really going to the metastore. Anything mounted above the cache would report its
            // hits instead; see GuardedPaimonCatalog.
            Catalog guarded = new GuardedPaimonCatalog(catalogName, unwrapped);
            if (!getPaimonOptions().get(CatalogOptions.CACHE_ENABLED)) {
                // no cache layer, hence nothing for the background refresh to track
                this.paimonNativeCatalog = PrivilegedCatalog.tryToCreate(guarded, getPaimonOptions());
                return paimonNativeCatalog;
            }
            CachingPaimonCatalog cachingCatalog = new CachingPaimonCatalog(catalogName, guarded, getPaimonOptions(),
                    refreshExecutor, tableCacheRefreshIntervalSec);
            this.paimonNativeCatalog = PrivilegedCatalog.tryToCreate(cachingCatalog, getPaimonOptions());
            GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor()
                    .registerPaimonCatalog(catalogName, cachingCatalog);
        }
        return paimonNativeCatalog;
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new PaimonMetadata(catalogName, hdfsEnvironment, getPaimonNativeCatalog(), connectorProperties);
    }

    @Override
    public void shutdown() {
        GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor().unRegisterPaimonCatalog(catalogName);
        refreshExecutor.shutdown();
    }
}
