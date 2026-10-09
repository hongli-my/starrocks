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
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class CosnFileIOTest {

    @Test
    public void testRegionOf() {
        Assertions.assertEquals("ap-guangzhou", CosnFileIO.regionOf("cos-internal.ap-guangzhou.tencentcos.cn"));
        Assertions.assertEquals("ap-shanghai", CosnFileIO.regionOf("https://cos.ap-shanghai.myqcloud.com"));
        Assertions.assertEquals("ap-beijing", CosnFileIO.regionOf("cos.ap-beijing.myqcloud.com"));
        // nothing to take the region from: the caller has to fail instead of guessing one, the
        // region is part of the request signature
        Assertions.assertNull(CosnFileIO.regionOf(null));
        Assertions.assertNull(CosnFileIO.regionOf("cos.tencentcos.cn"));
    }

    @Test
    public void testBucketAndKey() throws IOException {
        Path path = new Path("cosn://starrocksgzhongli-1258344700/paimon/db1.db/tbl1");
        Assertions.assertEquals("starrocksgzhongli-1258344700", CosnFileIO.bucketOf(path));
        Assertions.assertEquals("paimon/db1.db/tbl1", CosnFileIO.keyOf(path));

        // COS keys never carry a leading slash
        Assertions.assertEquals("", CosnFileIO.keyOf(new Path("cosn://bucket")));
        Assertions.assertThrows(IOException.class, () -> CosnFileIO.bucketOf(new Path("file:///tmp/x")));
    }

    @Test
    public void testLoaderRegistersCosnScheme() {
        CosnFileIOLoader loader = new CosnFileIOLoader();
        Assertions.assertEquals("cosn", loader.getScheme());
        // declaring no required option is deliberate: a Paimon REST catalog does not hand out
        // fs.cosn.bucket.region, and Paimon skips a loader whose required options are missing
        Assertions.assertTrue(loader.requiredOptions().isEmpty());
        Assertions.assertNotNull(loader.load(new Path("cosn://bucket/key")));
    }

    /**
     * Paimon refuses to run when two loaders claim the same scheme, so the META-INF/services entry
     * is load bearing and the scheme has to stay unique.
     */
    @Test
    public void testLoaderIsRegisteredExactlyOnce() {
        Map<String, FileIOLoader> loaders = new HashMap<>();
        for (FileIOLoader loader : ServiceLoader.load(FileIOLoader.class)) {
            Assertions.assertNull(loaders.put(loader.getScheme(), loader),
                    "two file io loaders claim scheme '" + loader.getScheme() + "'");
        }
        Assertions.assertTrue(loaders.containsKey("cosn"),
                "the cosn loader is missing from META-INF/services/org.apache.paimon.fs.FileIOLoader");
    }

    @Test
    public void testBuildClientNeedsCredentialsAndRegion() {
        Map<String, String> conf = new HashMap<>();
        conf.put(CosnFileIO.KEY_SECRET_KEY, "sk");
        Assertions.assertThrows(IllegalStateException.class, () -> CosnFileIO.buildClient(conf));

        conf.put(CosnFileIO.KEY_SECRET_ID, "ak");
        // no explicit region and no suffix to take one from
        Assertions.assertThrows(IllegalStateException.class, () -> CosnFileIO.buildClient(conf));

        conf.put(CosnFileIO.KEY_ENDPOINT_SUFFIX, "cos-internal.ap-guangzhou.tencentcos.cn");
        COSClient client = CosnFileIO.buildClient(conf);
        Assertions.assertNotNull(client);
        client.shutdown();
    }

    @Test
    public void testNotFoundOnlyMatchesMissingObjects() {
        CosServiceException missing = new CosServiceException("not found");
        missing.setStatusCode(404);
        Assertions.assertTrue(CosnFileIO.isNotFound(missing));

        CosServiceException denied = new CosServiceException("denied");
        denied.setStatusCode(403);
        Assertions.assertFalse(CosnFileIO.isNotFound(denied));

        // a transport failure must never be mistaken for an absent file, even though a port number
        // in the message can contain "404"
        Assertions.assertFalse(CosnFileIO.isNotFound(new CosClientException("connect timed out to cos:404")));
    }
}
