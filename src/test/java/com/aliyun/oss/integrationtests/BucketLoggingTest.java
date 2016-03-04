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

import static com.aliyun.oss.integrationtests.TestConstants.BUCKET_ACCESS_DENIED_ERR;
import static com.aliyun.oss.integrationtests.TestConstants.NO_SUCH_BUCKET_ERR;
import static com.aliyun.oss.integrationtests.TestConstants.INVALID_TARGET_BUCKET_FOR_LOGGING_ERR;
import static com.aliyun.oss.integrationtests.TestUtils.waitForCacheExpiration;

import junit.framework.Assert;

import org.junit.Test;

import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.BucketLoggingResult;
import com.aliyun.oss.model.SetBucketLoggingRequest;

public class BucketLoggingTest extends TestBase {

    @Test
    public void testNormalSetBucketLogging() {
        final String sourceBucket = getBucketName("normal-source");
        final String targetBucket = getBucketName("normal-target");
        final String targetPrefix = "normal-set-bucket-logging-prefix";
        
        try {
            client.createBucket(sourceBucket);
            client.createBucket(targetBucket);
            
            // Set target bucket not same as source bucket
            SetBucketLoggingRequest request = new SetBucketLoggingRequest(sourceBucket);
            request.setTargetBucket(targetBucket);
            request.setTargetPrefix(targetPrefix);
            client.setBucketLogging(request);
            
            BucketLoggingResult result = client.getBucketLogging(sourceBucket);
            Assert.assertEquals(targetBucket, result.getTargetBucket());
            Assert.assertEquals(targetPrefix, result.getTargetPrefix());
            
            client.deleteBucketLogging(sourceBucket);
            
            // Set target bucket same as source bucket
            request.setTargetBucket(sourceBucket);
            request.setTargetPrefix(targetPrefix);
            client.setBucketLogging(request);
            
            waitForCacheExpiration(5);
            
            result = client.getBucketLogging(sourceBucket);
            Assert.assertEquals(sourceBucket, result.getTargetBucket());
            Assert.assertEquals(targetPrefix, result.getTargetPrefix());
            
            client.deleteBucketLogging(sourceBucket);
            
            // Set target prefix null
            request.setTargetBucket(targetBucket);
            request.setTargetPrefix(null);
            client.setBucketLogging(request);
            
            result = client.getBucketLogging(sourceBucket);
            Assert.assertEquals(targetBucket, result.getTargetBucket());
            Assert.assertTrue(result.getTargetPrefix().isEmpty());
            
            client.deleteBucketLogging(sourceBucket);
            
            // Close bucket logging functionality
            request.setTargetBucket(null);
            request.setTargetPrefix(null);
            client.setBucketLogging(request);
            
            result = client.getBucketLogging(sourceBucket);
            Assert.assertTrue(result.getTargetBucket() == null);
            Assert.assertTrue(result.getTargetPrefix() == null);
            
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            client.deleteBucket(sourceBucket);
            client.deleteBucket(targetBucket);
        }
    }
    
    @Test
    public void testUnormalSetBucketLogging() {
        final String sourceBucket = getBucketName("unormal-source");
        final String targetBucket = getBucketName("unormal-target");
        final String targetPrefix = "unormal-set-bucket-logging-prefix";
        
        try {
            client.createBucket(sourceBucket);
            client.createBucket(targetBucket);
            
            // Set non-existent source bucket 
            final String nonexistentSourceBucket = getBucketName("non-exist-source");
            try {                
                SetBucketLoggingRequest request = new SetBucketLoggingRequest(nonexistentSourceBucket);
                request.setTargetBucket(targetBucket);
                request.setTargetPrefix(targetPrefix);
                client.setBucketLogging(request);
                
                Assert.fail("Set bucket logging should not be successful");
            } catch (OSSException e) {
                Assert.assertEquals(OSSErrorCode.NO_SUCH_BUCKET, e.getErrorCode());
                Assert.assertTrue(e.getMessage().startsWith(NO_SUCH_BUCKET_ERR));
            }
            
            // Set non-existent target bucket 
            final String nonexistentTargetBucket = getBucketName("non-exist-target");
            try {                
                SetBucketLoggingRequest request = new SetBucketLoggingRequest(sourceBucket);
                request.setTargetBucket(nonexistentTargetBucket);
                request.setTargetPrefix(targetPrefix);
                client.setBucketLogging(request);
                
                Assert.fail("Set bucket logging should not be successful");
            } catch (OSSException e) {
                Assert.assertEquals(OSSErrorCode.INVALID_TARGET_BUCKET_FOR_LOGGING, e.getErrorCode());
                Assert.assertTrue(e.getMessage().startsWith(INVALID_TARGET_BUCKET_FOR_LOGGING_ERR));                
            }
            
            // Set location of source bucket not same as target bucket
            final String targetBucketWithDiffLocation = getBucketName("target-bucket-with-diff-location");
            try {
                client.createBucket(targetBucketWithDiffLocation);
                
                SetBucketLoggingRequest request = new SetBucketLoggingRequest(sourceBucket);
                request.setTargetBucket(targetBucketWithDiffLocation);
                request.setTargetPrefix(targetPrefix);
                client.setBucketLogging(request);
                
                Assert.fail("Set bucket logging should not be successful");
            } catch (OSSException e) {
                Assert.assertEquals(OSSErrorCode.INVALID_TARGET_BUCKET_FOR_LOGGING, e.getErrorCode());
                Assert.assertTrue(e.getMessage().startsWith(INVALID_TARGET_BUCKET_FOR_LOGGING_ERR));
            } finally {
                client.deleteBucket(targetBucketWithDiffLocation);
            }
        } finally {
            client.deleteBucket(sourceBucket);
            client.deleteBucket(targetBucket);
        }
    }
    
    @Test
    public void testUnormalGetBucketLogging() {
        // Get non-existent bucket
        final String nonexistentBucket = getBucketName("non-exist");
        try {
            client.getBucketLogging(nonexistentBucket);
            Assert.fail("Get bucket logging should not be successful");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.NO_SUCH_BUCKET, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(NO_SUCH_BUCKET_ERR));
        }
        
        // Get bucket without ownership
        final String bucketWithoutOwnership = "oss";
        try {
            client.getBucketLogging(bucketWithoutOwnership);
            Assert.fail("Get bucket logging should not be successful");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.ACCESS_DENIED, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(BUCKET_ACCESS_DENIED_ERR));
        }
        
        // Get bucket without setting logging rule
        final String bucketWithoutLoggingRule = getBucketName("bucket-without-logging-rule");
        try {
            client.createBucket(bucketWithoutLoggingRule);
            
            BucketLoggingResult result = client.getBucketLogging(bucketWithoutLoggingRule);
            Assert.assertTrue(result.getTargetBucket() == null);
            Assert.assertTrue(result.getTargetPrefix() == null);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            client.deleteBucket(bucketWithoutLoggingRule);
        }
    }
    
    @Test
    public void testUnormalDeleteBucketLogging() {
        // Delete non-existent bucket
        final String nonexistentBucket = getBucketName("non-exist");
        try {
            client.deleteBucketLogging(nonexistentBucket);
            Assert.fail("Delete bucket logging should not be successful");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.NO_SUCH_BUCKET, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(NO_SUCH_BUCKET_ERR));
        }
        
        // Delete bucket without ownership
        final String bucketWithoutOwnership = "oss";
        try {
            client.deleteBucketLogging(bucketWithoutOwnership);
            Assert.fail("Delete bucket logging should not be successful");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.ACCESS_DENIED, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(BUCKET_ACCESS_DENIED_ERR));
        }
        
        // Delete bucket without setting logging rule
        final String bucketWithoutLoggingRule = getBucketName("bucket-without-logging-rule");
        try {
            client.createBucket(bucketWithoutLoggingRule);
            client.deleteBucketLogging(bucketWithoutLoggingRule);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            client.deleteBucket(bucketWithoutLoggingRule);
        }
    }
}
