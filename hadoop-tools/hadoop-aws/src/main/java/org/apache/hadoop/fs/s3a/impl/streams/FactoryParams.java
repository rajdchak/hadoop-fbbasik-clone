/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.impl.streams;

import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Factory parameters class to hold various parameters needed for stream creation.
 */
public class FactoryParams {
    private final S3AsyncClient s3AsyncClient;

    private FactoryParams(Builder builder) {
        this.s3AsyncClient = builder.s3AsyncClient;
    }

    public S3AsyncClient getS3AsyncClient() {
        return s3AsyncClient;
    }

    public static class Builder {
        private S3AsyncClient s3AsyncClient;

        public Builder withS3AsyncClient(S3AsyncClient s3AsyncClient) {
            this.s3AsyncClient = s3AsyncClient;
            return this;
        }

        public FactoryParams build() {
            return new FactoryParams(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
