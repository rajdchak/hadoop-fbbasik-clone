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

import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.service.AbstractService;

import static org.apache.hadoop.util.StringUtils.toLowerCase;

/**
 * Base implementation of {@link ObjectInputStreamFactory}.
 */
public abstract class AbstractObjectInputStreamFactory extends AbstractService
    implements ObjectInputStreamFactory {

  protected AbstractObjectInputStreamFactory(final String name) {
    super(name);
  }

  @Override
  public boolean hasCapability(final String capability) {
    switch (toLowerCase(capability)) {
    case StreamCapabilities.IOSTATISTICS:
    case StreamStatisticNames.STREAM_LEAKS:
      return true;
    default:
      return false;
    }
  }

}
