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

package com.starrocks.credential.tencent;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TencentCloudCredentialTest {

    @Test
    public void testStaticCredentialHasNoSessionToken() {
        TencentCloudCredential credential =
                new TencentCloudCredential("ak", "sk", "cos-internal.ap-guangzhou.tencentcos.cn");

        Map<String, String> properties = new HashMap<>();
        credential.toThrift(properties);
        Assertions.assertEquals("ak", properties.get("aws.s3.access_key"));
        Assertions.assertEquals("sk", properties.get("aws.s3.secret_key"));
        Assertions.assertEquals("cos-internal.ap-guangzhou.tencentcos.cn", properties.get("aws.s3.endpoint"));
        Assertions.assertFalse(properties.containsKey("aws.s3.session_token"));

        Configuration configuration = new Configuration();
        credential.applyToConfiguration(configuration);
        Assertions.assertNull(configuration.get("fs.cosn.userinfo.sessionToken"));
    }

    @Test
    public void testSessionTokenIsForwardedToBackend() {
        TencentCloudCredential credential =
                new TencentCloudCredential("ak", "sk", "cos-internal.ap-guangzhou.tencentcos.cn", "session-token");

        // the backend reads cosn:// as an S3 compatible fs, so the token travels as aws.s3.session_token
        Map<String, String> properties = new HashMap<>();
        credential.toThrift(properties);
        Assertions.assertEquals("session-token", properties.get("aws.s3.session_token"));

        Configuration configuration = new Configuration();
        credential.applyToConfiguration(configuration);
        Assertions.assertEquals("session-token", configuration.get("fs.cosn.userinfo.sessionToken"));
    }
}
