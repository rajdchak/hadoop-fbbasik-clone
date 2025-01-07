package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.audit.AuditConstants;
import org.apache.hadoop.fs.store.audit.HttpReferrerAuditHeader;
import software.amazon.s3.analyticsaccelerator.request.AuditHeaders;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;

import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_THREAD0;
import static org.apache.hadoop.fs.audit.AuditConstants.PARAM_TIMESTAMP;
import static org.apache.hadoop.fs.audit.CommonAuditContext.currentThreadID;

public class S3AAuditHeaders implements AuditHeaders {

    private final HttpReferrerAuditHeader referrer;

    public S3AAuditHeaders(HttpReferrerAuditHeader referrer) {
        this.referrer = referrer;
    }

    @Override
    public String modifyAndBuildReferrerHeader(GetRequest getRequestContext) {
        HttpReferrerAuditHeader copyReferrer = new HttpReferrerAuditHeader(this.referrer);
        copyReferrer.set(AuditConstants.PARAM_RANGE, getRequestContext.getRange().toHttpString());
        return copyReferrer.buildHttpReferrer();
    } 
}
