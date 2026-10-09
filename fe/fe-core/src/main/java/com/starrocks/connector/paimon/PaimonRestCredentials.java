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
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.table.Table;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_SECRET_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_SESSION_TOKEN;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.TENCENT_COS_ACCESS_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.TENCENT_COS_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.TENCENT_COS_SECRET_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.TENCENT_COS_SESSION_TOKEN;

/**
 * Builds the storage credentials a scan needs from a Paimon REST catalog's per-table data token.
 *
 * <p>A REST catalog that vends temporary credentials does not hand them out as catalog properties:
 * {@code RESTTokenFileIO} fetches (and refreshes) a short-lived token per table and merges it into
 * the options of the underlying {@link FileIO}. The frontend reads through that token, but the
 * backend reads data files on its own, so the credentials have to be forwarded with the scan.
 *
 * <p>The token carries the vendor's own option names ({@code fs.cosn.*} for Tencent COS,
 * {@code fs.s3a.*} for S3), which are translated here into the properties the catalog level
 * {@link CloudConfigurationFactory} understands.
 *
 * <p>Returns {@code null} when the catalog is not a data-token REST catalog, so callers can fall
 * back to the credentials configured on the catalog.
 */
public final class PaimonRestCredentials {
    private static final Logger LOG = LogManager.getLogger(PaimonRestCredentials.class);

    /** Keys a Paimon REST catalog uses to vendor S3 credentials through its data token. */
    private static final String S3_ACCESS_KEY = "fs.s3a.access.key";
    private static final String S3_SECRET_KEY = "fs.s3a.secret.key";
    private static final String S3_SESSION_TOKEN = "fs.s3a.session.token";
    /** Keys the frontend receives from GET /v1/config for the same storage. */
    private static final String S3_ENDPOINT = "fs.s3a.endpoint";
    private static final String S3_REGION = "fs.s3a.endpoint.region";

    private PaimonRestCredentials() {
    }

    public static CloudConfiguration buildCloudConfiguration(Table nativeTable, Catalog catalog) {
        if (nativeTable == null || catalog == null) {
            LOG.warn("Paimon table has no native table ({}), keeping the catalog level credentials",
                    nativeTable == null ? "nativeTable=null" : "catalog=null");
            return null;
        }
        FileIO fileIO;
        try {
            fileIO = nativeTable.fileIO();
        } catch (RuntimeException e) {
            LOG.warn("Failed to get the file io of paimon table {}, keeping the catalog level credentials",
                    nativeTable.name(), e);
            return null;
        }
        if (!(fileIO instanceof RESTTokenFileIO)) {
            // a filesystem/hive/jdbc catalog has no data token to forward
            LOG.debug("Paimon table {} file io is {}, no data token to forward", nativeTable.name(),
                    fileIO.getClass().getName());
            return null;
        }

        Map<String, String> token;
        try {
            token = ((RESTTokenFileIO) fileIO).validToken().token();
        } catch (RuntimeException e) {
            // validToken() refreshes the token over the network when it is about to expire, so a
            // failure here means the scan cannot be planned with usable credentials at all.
            throw new StarRocksConnectorException(
                    "Failed to fetch the paimon REST data token of table " + nativeTable.name(), e);
        }

        // Options carry both what GET /v1/config advertised and anything the user passed through
        // paimon.option.*, so the endpoint of the storage can be read from here.
        Map<String, String> catalogOptions = catalog.options();
        if (catalogOptions == null) {
            catalogOptions = Map.of();
        }
        Map<String, String> storageOptions = new HashMap<>();
        if (token.get(CosnFileIO.KEY_SECRET_ID) != null && token.get(CosnFileIO.KEY_SECRET_KEY) != null) {
            storageOptions.put(TENCENT_COS_ACCESS_KEY, token.get(CosnFileIO.KEY_SECRET_ID));
            storageOptions.put(TENCENT_COS_SECRET_KEY, token.get(CosnFileIO.KEY_SECRET_KEY));
            putIfPresent(storageOptions, TENCENT_COS_SESSION_TOKEN, token.get(CosnFileIO.KEY_SESSION_TOKEN));
            putIfPresent(storageOptions, TENCENT_COS_ENDPOINT, catalogOptions.get(CosnFileIO.KEY_ENDPOINT_SUFFIX));
        } else if (token.get(S3_ACCESS_KEY) != null && token.get(S3_SECRET_KEY) != null) {
            storageOptions.put(AWS_S3_ACCESS_KEY, token.get(S3_ACCESS_KEY));
            storageOptions.put(AWS_S3_SECRET_KEY, token.get(S3_SECRET_KEY));
            putIfPresent(storageOptions, AWS_S3_SESSION_TOKEN, token.get(S3_SESSION_TOKEN));
            putIfPresent(storageOptions, AWS_S3_ENDPOINT, catalogOptions.get(S3_ENDPOINT));
            putIfPresent(storageOptions, AWS_S3_REGION, catalogOptions.get(S3_REGION));
        } else {
            LOG.warn("Paimon REST data token of table {} carries no recognizable credentials (keys: {}), "
                    + "keeping the catalog level credentials", nativeTable.name(), token.keySet());
            return null;
        }

        CloudConfiguration configuration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(storageOptions);
        // The chain ends with a catch all that returns a DEFAULT configuration, which the backend
        // would ignore.
        if (configuration.getCloudType() == CloudType.DEFAULT) {
            if (storageOptions.containsKey(TENCENT_COS_ACCESS_KEY)) {
                // Without an endpoint the backend would silently fall back to the AWS default
                // endpoint and report a confusing "bucket does not exist" instead.
                throw new StarRocksConnectorException("The paimon REST catalog vends COS credentials for table "
                        + nativeTable.name() + " but does not provide " + CosnFileIO.KEY_ENDPOINT_SUFFIX
                        + ", so the backend cannot reach COS. Set it with \"paimon.option."
                        + CosnFileIO.KEY_ENDPOINT_SUFFIX + "\".");
            }
            LOG.warn("Paimon REST data token of table {} could not be turned into a cloud configuration "
                    + "(keys: {}), keeping the catalog level credentials", nativeTable.name(), token.keySet());
            return null;
        }
        LOG.debug("Forwarding the paimon REST data token of table {} to the backend as {}",
                nativeTable.name(), configuration.getCloudType());
        return configuration;
    }

    /** Never forwards an empty value: the backend would only treat it as "unset" at best. */
    private static void putIfPresent(Map<String, String> target, String key, String value) {
        if (value != null && !value.isEmpty()) {
            target.put(key, value);
        }
    }
}
