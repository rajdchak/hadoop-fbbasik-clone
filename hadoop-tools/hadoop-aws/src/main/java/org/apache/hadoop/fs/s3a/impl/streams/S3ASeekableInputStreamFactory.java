/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a.impl.streams;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3ASeekableInputStream;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.Constants.*;

public class S3ASeekableInputStreamFactory extends AbstractObjectInputStreamFactory {

    private final S3AsyncClient s3AsyncClient;
    private S3SeekableInputStreamFactory s3SeekableInputStreamFactory;

    public S3ASeekableInputStreamFactory(S3AsyncClient s3AsyncClient) {
        super("S3ASeekableInputStreamFactory");
        this.s3AsyncClient = s3AsyncClient;
    }

    @Override
    protected void serviceInit(final Configuration conf) throws Exception {
        super.serviceInit(conf);
        ConnectorConfiguration configuration = new ConnectorConfiguration(conf,
                ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX);
        S3SeekableInputStreamConfiguration seekableInputStreamConfiguration =
                S3SeekableInputStreamConfiguration.fromConfiguration(configuration);
        this.s3SeekableInputStreamFactory =
                new S3SeekableInputStreamFactory(
                        new S3SdkObjectClient(this.s3AsyncClient),
                        seekableInputStreamConfiguration);
    }

    @Override
    public ObjectInputStream readObject(final ObjectReadParameters parameters) throws IOException {
        return new S3ASeekableInputStream(
                parameters,
                s3SeekableInputStreamFactory);
    }

    /**
     * Get the number of background threads required for this factory.
     * @return the count of background threads.
     */
    @Override
    public StreamThreadOptions threadRequirements() {
        return new StreamThreadOptions(0, 0, false, false);
    }


}
