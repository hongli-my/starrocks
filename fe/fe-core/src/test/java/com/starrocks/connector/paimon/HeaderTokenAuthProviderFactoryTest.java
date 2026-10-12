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

import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.AuthProviderFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class HeaderTokenAuthProviderFactoryTest {

    /**
     * Paimon resolves {@code token.provider} through a service loader, so the
     * {@code META-INF/services/org.apache.paimon.factories.Factory} entry is load bearing: without
     * it the client fails with "Could not find any factory for identifier 'header'".
     */
    @Test
    public void testFactoryIsDiscoverable() {
        AuthProviderFactory factory = FactoryUtil.discoverFactory(
                AuthProviderFactory.class.getClassLoader(), AuthProviderFactory.class,
                HeaderTokenAuthProviderFactory.IDENTIFIER);
        Assertions.assertInstanceOf(HeaderTokenAuthProviderFactory.class, factory);
    }

    @Test
    public void testProviderLeavesTheHeadersUntouched() {
        AuthProvider provider = new HeaderTokenAuthProviderFactory().create(new Options());
        Map<String, String> headers = Map.of("X-Paimon-Bypass-Token", "init_token");

        Map<String, String> merged = provider.mergeAuthHeader(headers, null);
        // the catalog's own header authenticates, an Authorization header must not be added
        Assertions.assertEquals(headers, merged);
        Assertions.assertFalse(merged.containsKey("Authorization"));
    }
}
