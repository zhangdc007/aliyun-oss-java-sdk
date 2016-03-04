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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.oss.integrationtests;

import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.utils.DateUtil;
import com.aliyun.oss.common.utils.HttpHeaders;
import com.aliyun.oss.common.utils.IOUtils;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.*;
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static com.aliyun.oss.integrationtests.TestConstants.*;
import static com.aliyun.oss.integrationtests.TestUtils.*;
import static com.aliyun.oss.internal.OSSConstants.DEFAULT_OBJECT_CONTENT_TYPE;

public class GetObjectTest extends TestBase {
    private final static String DOWNLOAD_DIR = "./downloads/";

    @Test
    public void testGetObjectByRange() {
        final String key = "get-object-by-range";
        final long inputStreamLength = 128 * 1024; //128KB
        
        try {
            client.putObject(bucketName, key, genFixedLengthInputStream(inputStreamLength), null);
            
            // Normal range [a-b]
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
            getObjectRequest.setRange(0, inputStreamLength / 2 - 1);
            OSSObject o = client.getObject(getObjectRequest);
            Assert.assertEquals(inputStreamLength / 2, o.getObjectMetadata().getContentLength());
            
            // Start to [a-]
            getObjectRequest.setRange(inputStreamLength / 2, -1);
            o = client.getObject(getObjectRequest);
            Assert.assertEquals(inputStreamLength / 2, o.getObjectMetadata().getContentLength());
            
            // To end [-b]
            getObjectRequest.setRange(-1, inputStreamLength / 4);
            o = client.getObject(getObjectRequest);
            Assert.assertEquals(inputStreamLength / 4, o.getObjectMetadata().getContentLength());
            
            // To end [-b] (b = 0)
            try {
                getObjectRequest.setRange(-1, 0);
                o = client.getObject(getObjectRequest);
                Assert.fail("Get object should not be successful");
            } catch (OSSException e) {
                Assert.assertEquals(OSSErrorCode.INVALID_RANGE, e.getErrorCode());
            }
            
            // Invalid range [-1, -1]
            getObjectRequest.setRange(-1, -1);
            o = client.getObject(getObjectRequest);
            Assert.assertEquals(inputStreamLength, o.getObjectMetadata().getContentLength());
            
            // Invalid range start > end, ignore it and just get entire object
            getObjectRequest.setRange(inputStreamLength / 2, inputStreamLength / 4);
            o = client.getObject(getObjectRequest);
            Assert.assertEquals(inputStreamLength, o.getObjectMetadata().getContentLength());
            
            // Invalid range exceeding object's max length, ignore it and just get entire object
            getObjectRequest.setRange(0, inputStreamLength * 2 - 1);
            o = client.getObject(getObjectRequest);
            Assert.assertEquals(inputStreamLength, o.getObjectMetadata().getContentLength());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
    
    @Test
    public void testOverridedGetObject() {
        final String key = "overrided-get-object";
        final long inputStreamLength = 128 * 1024; //128KB
        
        try {
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, 
                    genFixedLengthInputStream(inputStreamLength), null);
            client.putObject(putObjectRequest);
            
            // Override 1
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
            OSSObject o = client.getObject(getObjectRequest);
            Assert.assertEquals(bucketName, o.getBucketName());
            Assert.assertEquals(key, o.getKey());
            Assert.assertEquals(inputStreamLength, o.getObjectMetadata().getContentLength());
            
            // Override 2
            o = client.getObject(bucketName, key);
            Assert.assertEquals(bucketName, o.getBucketName());
            Assert.assertEquals(key, o.getKey());
            Assert.assertEquals(inputStreamLength, o.getObjectMetadata().getContentLength());
            
            // Override 3
            final String filePath = genFixedLengthFile(0);
            ObjectMetadata metadata = client.getObject(getObjectRequest, new File(filePath));
            Assert.assertEquals(inputStreamLength, metadata.getContentLength());
            Assert.assertEquals(inputStreamLength, new File(filePath).length());
            
            metadata = client.getObjectMetadata(bucketName, key);
            Assert.assertEquals(inputStreamLength, metadata.getContentLength());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
    
    @Test
    public void testGetObjectWithSpecialChars() {
        final String key = "测\\r试-中.~,+\"'*&￥#@%！（文）+字符|？/.zip";
        final long inputStreamLength = 128 * 1024; //128KB
        //TODO: With chinese characters will be failed. 
        final String metaKey0 = "tag";
        final String metaValue0 = "元值0";
        
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType(DEFAULT_OBJECT_CONTENT_TYPE);
            metadata.addUserMetadata(metaKey0, metaValue0);
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, 
                    genFixedLengthInputStream(inputStreamLength), metadata);
            client.putObject(putObjectRequest);
            
            // Override 1
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
            OSSObject o = client.getObject(getObjectRequest);
            Assert.assertEquals(bucketName, o.getBucketName());
            Assert.assertEquals(key, o.getKey());
            metadata = o.getObjectMetadata();
            Assert.assertEquals(DEFAULT_OBJECT_CONTENT_TYPE, metadata.getContentType());
            Assert.assertEquals(metaValue0, metadata.getUserMetadata().get(metaKey0));
            Assert.assertEquals(inputStreamLength, metadata.getContentLength());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
    
    @Test
    public void testGetObjectByUrlsignature() {    
        final String key = "put-object-by-urlsignature";
        final String expirationString = "Sun, 12 Apr 2016 12:00:00 GMT";
        final long inputStreamLength = 128 * 1024; //128KB
        final long firstByte= inputStreamLength / 2;
        final long lastByte = inputStreamLength - 1;
        
        try {
            client.putObject(bucketName, key, genFixedLengthInputStream(inputStreamLength), null);
            
            GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, key);
            Date expiration = DateUtil.parseRfc822Date(expirationString);
            request.setExpiration(expiration);
            request.setContentType(DEFAULT_OBJECT_CONTENT_TYPE);
            URL signedUrl = client.generatePresignedUrl(request);
            
            Map<String, String> requestHeaders = new HashMap<String, String>();
            requestHeaders.put(HttpHeaders.RANGE, String.format("bytes=%d-%d", firstByte, lastByte));
            requestHeaders.put(HttpHeaders.CONTENT_TYPE, DEFAULT_OBJECT_CONTENT_TYPE);
            OSSObject o = client.getObject(signedUrl, requestHeaders);
            
            try {
                int bytesRead = -1;
                int totalBytes = 0;
                byte[] buffer = new byte[4096];
                while ((bytesRead = o.getObjectContent().read(buffer)) != -1) {
                    totalBytes += bytesRead;
                }
              
                Assert.assertEquals((lastByte - firstByte + 1), totalBytes);
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            } finally {
                IOUtils.safeClose(o.getObjectContent());
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
    
    @Test
    public void testUnormalGetObject() throws Exception {
        final Date beforeModifiedTime = new Date();
        Thread.sleep(1000);
        
        // Try to get object under nonexistent bucket
        final String key = "unormal-get-object";
        final String nonexistentBucket = "nonexistent-bukcet";
        try {
            client.getObject(nonexistentBucket, key);
            Assert.fail("Get object should not be successful");
        } catch (OSSException ex) {
            Assert.assertEquals(OSSErrorCode.NO_SUCH_BUCKET, ex.getErrorCode());
            Assert.assertTrue(ex.getMessage().startsWith(NO_SUCH_BUCKET_ERR));
        }
        
        // Try to get nonexistent object
        final String nonexistentKey = "nonexistent-object";
        try {
            client.getObject(bucketName, nonexistentKey);
            Assert.fail("Get object should not be successful");
        } catch (OSSException ex) {
            Assert.assertEquals(OSSErrorCode.NO_SUCH_KEY, ex.getErrorCode());
            Assert.assertTrue(ex.getMessage().startsWith(NO_SUCH_KEY_ERR));
        }
        
        String eTag = null;
        try {
            PutObjectResult result = client.putObject(bucketName, key, genFixedLengthInputStream(1024), null);
            eTag = result.getETag();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        
        // Matching ETag Constraints
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
        List<String> matchingETagConstraints = new ArrayList<String>();
        OSSObject o = null;
        matchingETagConstraints.add(eTag);
        getObjectRequest.setMatchingETagConstraints(matchingETagConstraints);
        try {
            o = client.getObject(getObjectRequest);
            Assert.assertEquals(eTag, o.getObjectMetadata().getETag());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            getObjectRequest.setMatchingETagConstraints(null);
        }
        
        matchingETagConstraints.clear();
        matchingETagConstraints.add("nonmatching-etag");
        getObjectRequest.setMatchingETagConstraints(matchingETagConstraints);
        try {
            o = client.getObject(getObjectRequest);
            Assert.fail("Get object should not be successful.");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.PRECONDITION_FAILED, e.getErrorCode());
        } finally {
            getObjectRequest.setMatchingETagConstraints(null);
        }
        
        // Non-Matching ETag Constraints
        List<String> nonmatchingETagConstraints = new ArrayList<String>();
        nonmatchingETagConstraints.add("nonmatching-etag");
        getObjectRequest.setNonmatchingETagConstraints(nonmatchingETagConstraints);
        try {
            o = client.getObject(getObjectRequest);
            Assert.assertEquals(eTag, o.getObjectMetadata().getETag());
        } catch (OSSException e) {
            Assert.fail(e.getMessage());
        } finally {
            getObjectRequest.setNonmatchingETagConstraints(null);
        }
        
        nonmatchingETagConstraints.clear();
        nonmatchingETagConstraints.add(eTag);
        getObjectRequest.setNonmatchingETagConstraints(nonmatchingETagConstraints);
        try {
            o = client.getObject(getObjectRequest);
            Assert.fail("Get object should not be successful.");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.NOT_MODIFIED, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(NOT_MODIFIED_ERR));
        } finally {
            getObjectRequest.setNonmatchingETagConstraints(null);
        }
        
        // Unmodified Since Constraint
        Date unmodifiedSinceConstraint = new Date();
        getObjectRequest.setUnmodifiedSinceConstraint(unmodifiedSinceConstraint);
        try {
            o = client.getObject(getObjectRequest);
            Assert.assertEquals(eTag, o.getObjectMetadata().getETag());
        } catch (OSSException e) {
            Assert.fail(e.getMessage());
        } finally {
            getObjectRequest.setUnmodifiedSinceConstraint(null);
        }
        
        unmodifiedSinceConstraint = beforeModifiedTime;
        getObjectRequest.setUnmodifiedSinceConstraint(unmodifiedSinceConstraint);
        try {
            o = client.getObject(getObjectRequest);
            Assert.fail("Get object should not be successful.");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.PRECONDITION_FAILED, e.getErrorCode());
        } finally {
            getObjectRequest.setUnmodifiedSinceConstraint(null);
        }
        
        // Modified Since Constraint
        Date modifiedSinceConstraint = beforeModifiedTime;
        getObjectRequest.setModifiedSinceConstraint(modifiedSinceConstraint);
        try {
            o = client.getObject(getObjectRequest);
            Assert.assertEquals(eTag, o.getObjectMetadata().getETag());
        } catch (OSSException e) {
            Assert.fail(e.getMessage());
        } finally {
            getObjectRequest.setModifiedSinceConstraint(null);
        }
        
        modifiedSinceConstraint = new Date();
        getObjectRequest.setModifiedSinceConstraint(modifiedSinceConstraint);
        try {
            o = client.getObject(getObjectRequest);
            Assert.fail("Get object should not be successful.");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.NOT_MODIFIED, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(NOT_MODIFIED_ERR));
        } finally {
            getObjectRequest.setModifiedSinceConstraint(null);
        }
    }
    
    @Test
    public void testGetObjectMetadataWithIllegalExpires() {
        final String key = "get-object-with-illegal-expires";
        final long inputStreamLength = 128 * 1024; //128KB
        final String illegalExpires = "2015-10-01 00:00:00";
        
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setHeader(OSSHeaders.EXPIRES, illegalExpires);
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, 
                    genFixedLengthInputStream(inputStreamLength), metadata);
            client.putObject(putObjectRequest);
            
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
            OSSObject o = client.getObject(getObjectRequest);
            try {
                o.getObjectMetadata().getExpirationTime();
                Assert.fail("Get expiration time should not be successful.");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof ParseException);
                Assert.assertEquals("Unparseable date: \"2015-10-01 00:00:00\"", e.getMessage());
            }
            
            String rawExpiresValue = o.getObjectMetadata().getRawExpiresValue();
            Assert.assertEquals(illegalExpires, rawExpiresValue);
            
            metadata = client.getObjectMetadata(bucketName, key);
            try {
                metadata.getExpirationTime();
                Assert.fail("Get expiration time should not be successful.");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof ParseException);
                Assert.assertEquals("Unparseable date: \"2015-10-01 00:00:00\"", e.getMessage());
            }
            
            rawExpiresValue = o.getObjectMetadata().getRawExpiresValue();
            Assert.assertEquals(illegalExpires, rawExpiresValue);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
