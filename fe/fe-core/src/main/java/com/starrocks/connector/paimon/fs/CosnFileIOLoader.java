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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;

/**
 * Makes {@link CosnFileIO} discoverable by Paimon's {@code ResolvingFileIO}.
 *
 * <p>Registered through {@code META-INF/services/org.apache.paimon.fs.FileIOLoader}. No required
 * options are declared on purpose: Paimon's own COS loader asks for {@code fs.cosn.bucket.region},
 * which a Paimon REST catalog does not hand out, and a missing required option makes Paimon skip
 * the loader.
 */
public class CosnFileIOLoader implements FileIOLoader {
    private static final long serialVersionUID = 1L;

    @Override
    public String getScheme() {
        return "cosn";
    }

    @Override
    public FileIO load(Path path) {
        return new CosnFileIO();
    }
}
