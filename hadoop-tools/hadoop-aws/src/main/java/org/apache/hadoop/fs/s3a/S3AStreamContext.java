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
