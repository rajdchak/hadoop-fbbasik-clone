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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.audit.AuditConstants;
import org.apache.hadoop.fs.store.audit.HttpReferrerAuditHeader;

import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;

public class S3AStreamContext implements StreamContext {

    private final HttpReferrerAuditHeader referrer;

    public S3AStreamContext(HttpReferrerAuditHeader referrer) {
        this.referrer = referrer;
    }

    @Override
    public String modifyAndBuildReferrerHeader(GetRequest getRequestContext) {
        HttpReferrerAuditHeader copyReferrer = new HttpReferrerAuditHeader(this.referrer);
        copyReferrer.set(AuditConstants.PARAM_RANGE, getRequestContext.getRange().toHttpString());
        return copyReferrer.buildHttpReferrer();
    }  

}
