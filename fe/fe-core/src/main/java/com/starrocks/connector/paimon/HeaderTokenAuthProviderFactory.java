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

import org.apache.paimon.options.Options;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.AuthProviderFactory;

/**
 * A token provider for Paimon REST catalog servers that authenticate on a header of their own,
 * such as {@code X-Paimon-Bypass-Token}.
 *
 * <p>Paimon refuses to build a client unless {@code token.provider} is set, but every provider it
 * ships adds an {@code Authorization} header. This one adds nothing: the token is already put on
 * every request through the {@code header.<name>} options the catalog sets. Keeping the request
 * free of an {@code Authorization} header matters for a server that accepts nothing but its own
 * header.
 *
 * <p>Discovered through {@code META-INF/services/org.apache.paimon.factories.Factory}.
 */
public class HeaderTokenAuthProviderFactory implements AuthProviderFactory {
    public static final String IDENTIFIER = "header";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public AuthProvider create(Options options) {
        // The headers of the request already carry the token, baseHeader is returned untouched.
        return (baseHeader, restAuthParameter) -> baseHeader;
    }
}
