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

package com.starrocks.connector.paimon.fs;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.region.Region;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A read-only Paimon {@link FileIO} for the Tencent COS {@code cosn} scheme.
 *
 * <p>Paimon's own COS support ({@code paimon-cosn}) is built on Hadoop's {@code CosNFileSystem},
 * which only accepts static secret id / secret key and therefore cannot use the short-lived STS
 * credentials that a Paimon REST catalog hands out through its data-token endpoint. This
 * implementation talks to the COS SDK directly, so the {@code fs.cosn.userinfo.*} options merged
 * into the catalog context by {@code RESTTokenFileIO} can be applied as they are.
 *
 * <p>Note: it is registered as the loader for the {@code cosn} scheme, and Paimon refuses to run
 * when two loaders claim the same scheme ("Multiple FileIO for scheme"). Paimon's own
 * {@code paimon-cosn} must therefore never be put on the frontend classpath next to this class.
 *
 * <p>Only the methods needed to plan reads are implemented; the connector treats the catalog as
 * read-only, so writing is rejected.
 */
public class CosnFileIO implements FileIO, Serializable {
    private static final long serialVersionUID = 1L;

    public static final String KEY_SECRET_ID = "fs.cosn.userinfo.secretId";
    public static final String KEY_SECRET_KEY = "fs.cosn.userinfo.secretKey";
    public static final String KEY_SESSION_TOKEN = "fs.cosn.userinfo.sessionToken";
    public static final String KEY_REGION = "fs.cosn.bucket.region";
    public static final String KEY_ENDPOINT_SUFFIX = "fs.cosn.bucket.endpoint_suffix";

    private static final String COSN_PREFIX = "fs.cosn.";

    private final Map<String, String> conf = new HashMap<>();
    private transient volatile COSClient client;

    @Override
    public void configure(CatalogContext context) {
        conf.clear();
        for (String key : context.options().keySet()) {
            if (!key.startsWith(COSN_PREFIX)) {
                continue;
            }
            String value = context.options().get(key);
            if (value != null && !value.isEmpty()) {
                conf.put(key, value);
            }
        }
    }

    @Override
    public boolean isObjectStore() {
        return true;
    }

    private COSClient client() {
        COSClient local = client;
        if (local != null) {
            return local;
        }
        synchronized (this) {
            if (client == null) {
                client = buildClient(conf);
            }
            return client;
        }
    }

    static COSClient buildClient(Map<String, String> conf) {
        String secretId = conf.get(KEY_SECRET_ID);
        String secretKey = conf.get(KEY_SECRET_KEY);
        if (secretId == null || secretKey == null) {
            throw new IllegalStateException(
                    "cosn FileIO requires both " + KEY_SECRET_ID + " and " + KEY_SECRET_KEY);
        }
        String sessionToken = conf.get(KEY_SESSION_TOKEN);
        COSCredentials credentials = sessionToken == null
                ? new BasicCOSCredentials(secretId, secretKey)
                : new BasicSessionCredentials(secretId, secretKey, sessionToken);

        String region = conf.get(KEY_REGION);
        if (region == null) {
            region = regionOf(conf.get(KEY_ENDPOINT_SUFFIX));
        }
        if (region == null) {
            // The region takes part in the request signature, so guessing it would only surface as
            // an opaque 403 much later.
            throw new IllegalStateException("cosn FileIO cannot determine the COS region, set "
                    + KEY_REGION + " or use an endpoint suffix that contains it");
        }
        ClientConfig clientConfig = new ClientConfig(new Region(region));
        clientConfig.setHttpProtocol(HttpProtocol.https);
        String endpointSuffix = conf.get(KEY_ENDPOINT_SUFFIX);
        if (endpointSuffix != null) {
            // The catalog hands out a bare suffix, while the SDK wants host only.
            clientConfig.setEndPointSuffix(endpointSuffix.replaceFirst("^https?://", ""));
        }
        return new COSClient(credentials, clientConfig);
    }

    /**
     * Extracts the region from an endpoint suffix such as {@code cos.ap-guangzhou.myqcloud.com},
     * or {@code null} when the suffix does not carry one.
     */
    static String regionOf(String endpointSuffix) {
        if (endpointSuffix != null) {
            for (String part : endpointSuffix.replaceFirst("^https?://", "").split("\\.")) {
                if (part.startsWith("ap-") || part.startsWith("na-") || part.startsWith("eu-")
                        || part.startsWith("sa-")) {
                    return part;
                }
            }
        }
        return null;
    }

    static String bucketOf(Path path) throws IOException {
        String authority = path.toUri().getAuthority();
        if (authority == null || authority.isEmpty()) {
            throw new IOException("cosn path does not contain a bucket: " + path);
        }
        return authority;
    }

    static String keyOf(Path path) {
        String p = path.toUri().getPath();
        if (p == null) {
            return "";
        }
        return p.startsWith("/") ? p.substring(1) : p;
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        return new CosnSeekableInputStream(bucketOf(path), keyOf(path));
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) {
        throw new UnsupportedOperationException("cosn FileIO is read-only, cannot write " + path);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        String bucket = bucketOf(path);
        String key = keyOf(path);
        try {
            ObjectMetadata metadata = client().getObjectMetadata(bucket, key);
            return new CosnFileStatus(path, metadata.getContentLength(), false,
                    metadata.getLastModified() == null ? 0L : metadata.getLastModified().getTime());
        } catch (CosClientException e) {
            if (!isNotFound(e)) {
                throw new IOException("failed to get cosn file status of " + path, e);
            }
        }
        // COS has no real directories; a key is a directory when something lives under it.
        if (key.isEmpty() || listFirst(bucket, key + "/") != null) {
            return new CosnFileStatus(path, 0L, true, 0L);
        }
        throw new FileNotFoundException(path.toString());
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        String bucket = bucketOf(path);
        String key = keyOf(path);
        String prefix = key.isEmpty() ? "" : key + "/";
        List<FileStatus> statuses = new ArrayList<>();
        String marker = null;
        do {
            ObjectListing listing = client().listObjects(
                    new ListObjectsRequest(bucket, prefix, marker, "/", 1000));
            for (String commonPrefix : listing.getCommonPrefixes()) {
                String trimmed = commonPrefix.endsWith("/")
                        ? commonPrefix.substring(0, commonPrefix.length() - 1) : commonPrefix;
                statuses.add(new CosnFileStatus(new Path(path, trimmed.substring(prefix.length())), 0L, true, 0L));
            }
            for (COSObjectSummary summary : listing.getObjectSummaries()) {
                if (summary.getKey().equals(prefix) || summary.getKey().endsWith("/")) {
                    continue;
                }
                statuses.add(new CosnFileStatus(new Path(path, summary.getKey().substring(prefix.length())),
                        summary.getSize(), false,
                        summary.getLastModified() == null ? 0L : summary.getLastModified().getTime()));
            }
            String next = listing.isTruncated() ? listing.getNextMarker() : null;
            // defensive: a marker that does not advance would list forever
            if (next != null && next.equals(marker)) {
                break;
            }
            marker = next;
        } while (marker != null);
        return statuses.toArray(new FileStatus[0]);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        String bucket = bucketOf(path);
        String key = keyOf(path);
        try {
            client().getObjectMetadata(bucket, key);
            return true;
        } catch (CosClientException e) {
            if (!isNotFound(e)) {
                throw new IOException("failed to check existence of " + path, e);
            }
        }
        return key.isEmpty() || listFirst(bucket, key + "/") != null;
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        try {
            client().deleteObject(bucketOf(path), keyOf(path));
            return true;
        } catch (CosClientException e) {
            return false;
        }
    }

    @Override
    public boolean mkdirs(Path path) {
        // A flat object store has no directories to create.
        return true;
    }

    @Override
    public boolean rename(Path src, Path dst) {
        return false;
    }

    @Override
    public void close() {
        COSClient local = client;
        if (local != null) {
            local.shutdown();
            client = null;
        }
    }

    private String listFirst(String bucket, String prefix) throws IOException {
        try {
            ObjectListing listing = client().listObjects(
                    new ListObjectsRequest(bucket, prefix, null, null, 1));
            return listing.getObjectSummaries().isEmpty()
                    ? null : listing.getObjectSummaries().get(0).getKey();
        } catch (CosClientException e) {
            if (isNotFound(e)) {
                return null;
            }
            throw asIOException(e);
        }
    }

    /**
     * A missing object or bucket is a normal outcome while probing paths. Anything else, including
     * a transport failure, must be reported instead of being mistaken for an absent file.
     */
    static boolean isNotFound(CosClientException e) {
        return e instanceof CosServiceException serviceException && serviceException.getStatusCode() == 404;
    }

    private static IOException asIOException(Throwable e) {
        return e instanceof IOException ? (IOException) e : new IOException(e.getMessage(), e);
    }

    private final class CosnSeekableInputStream extends SeekableInputStream {
        private final String bucket;
        private final String key;
        private InputStream in;
        private long pos;
        private long length = -1L;

        CosnSeekableInputStream(String bucket, String key) throws IOException {
            this.bucket = bucket;
            this.key = key;
            open(0L);
        }

        private void open(long start) throws IOException {
            try {
                GetObjectRequest request = new GetObjectRequest(bucket, key);
                if (start > 0) {
                    if (length < 0) {
                        length = client().getObjectMetadata(bucket, key).getContentLength();
                    }
                    if (start >= length) {
                        in = new ByteArrayInputStream(new byte[0]);
                        pos = start;
                        return;
                    }
                    request.setRange(start, length - 1);
                }
                COSObject object = client().getObject(request);
                if (length < 0) {
                    length = object.getObjectMetadata().getContentLength();
                }
                in = object.getObjectContent();
                pos = start;
            } catch (CosClientException e) {
                throw asIOException(e);
            }
        }

        @Override
        public void seek(long seekPos) throws IOException {
            if (seekPos < 0) {
                throw new IOException("negative seek offset " + seekPos);
            }
            if (seekPos == pos) {
                return;
            }
            closeQuietly();
            open(seekPos);
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public int read() throws IOException {
            int c = in.read();
            if (c >= 0) {
                pos++;
            }
            return c;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = in.read(b, off, len);
            if (n > 0) {
                pos += n;
            }
            return n;
        }

        @Override
        public void close() {
            closeQuietly();
        }

        private void closeQuietly() {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ignored) {
                // ignore, the stream is being discarded
            }
        }
    }

    private static final class CosnFileStatus implements FileStatus {
        private final Path path;
        private final long len;
        private final boolean dir;
        private final long modificationTime;

        CosnFileStatus(Path path, long len, boolean dir, long modificationTime) {
            this.path = path;
            this.len = len;
            this.dir = dir;
            this.modificationTime = modificationTime;
        }

        @Override
        public long getLen() {
            return len;
        }

        @Override
        public boolean isDir() {
            return dir;
        }

        @Override
        public Path getPath() {
            return path;
        }

        @Override
        public long getModificationTime() {
            return modificationTime;
        }
    }
}
