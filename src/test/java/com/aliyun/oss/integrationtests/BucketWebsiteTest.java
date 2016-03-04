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
import static com.aliyun.oss.integrationtests.TestConstants.NO_SUCH_WEBSITE_CONFIGURATION_ERR;
import static com.aliyun.oss.integrationtests.TestUtils.waitForCacheExpiration;

import junit.framework.Assert;

import org.junit.Test;

import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.BucketWebsiteResult;
import com.aliyun.oss.model.SetBucketWebsiteRequest;

public class BucketWebsiteTest extends TestBase {

    @Test
    public void testNormalSetBucketWebsite() {
        final String bucketName = getBucketName("normal");
        final String indexDocument = "index.html";
        final String errorDocument = "error.html";
        
        try {
            client.createBucket(bucketName);
            
            // Set both index document and error document
            SetBucketWebsiteRequest request = new SetBucketWebsiteRequest(bucketName);
            request.setIndexDocument(indexDocument);
            request.setErrorDocument(errorDocument);
            client.setBucketWebsite(request);
            
            waitForCacheExpiration(5);
            
            BucketWebsiteResult result = client.getBucketWebsite(bucketName);
            Assert.assertEquals(indexDocument, result.getIndexDocument());
            Assert.assertEquals(errorDocument, result.getErrorDocument());
            
            client.deleteBucketWebsite(bucketName);
            
            // Set index document only
            request = new SetBucketWebsiteRequest(bucketName);
            request.setIndexDocument(indexDocument);
            request.setErrorDocument(null);
            client.setBucketWebsite(request);
            
            waitForCacheExpiration(5);
            
            result = client.getBucketWebsite(bucketName);
            Assert.assertEquals(indexDocument, result.getIndexDocument());
            Assert.assertTrue(result.getErrorDocument() == null);
            
            client.deleteBucketWebsite(bucketName);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            client.deleteBucket(bucketName);
        }
    }
    
    @Test
    public void testUnormalSetBucketWebsite() {
        final String bucketName = getBucketName("unormal");
        final String indexDocument = "index.html";
        final String errorDocument = "error.html";
        
        try {
            client.createBucket(bucketName);
            
            // Set non-existent bucket 
            final String nonexistentBucket = getBucketName("nonexistent-bucket");
            try {                
                SetBucketWebsiteRequest request = new SetBucketWebsiteRequest(nonexistentBucket);
                request.setIndexDocument(indexDocument);
                request.setErrorDocument(errorDocument);
                client.setBucketWebsite(request);
                
                Assert.fail("Set bucket website should not be successful");
            } catch (OSSException e) {
                Assert.assertEquals(OSSErrorCode.NO_SUCH_BUCKET, e.getErrorCode());
                Assert.assertTrue(e.getMessage().startsWith(NO_SUCH_BUCKET_ERR));
            }
            
            // Set index document null
            try {                
                SetBucketWebsiteRequest request = new SetBucketWebsiteRequest(nonexistentBucket);
                request.setIndexDocument(null);
                request.setErrorDocument(errorDocument);
                client.setBucketWebsite(request);
                
                Assert.fail("Set bucket website should not be successful");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof NullPointerException);
            }
        } finally {
            client.deleteBucket(bucketName);
        }
    }
    
    @Test
    public void testUnormalGetBucketWebsite() {
        // Get non-existent bucket
        final String nonexistentBucket = getBucketName("non-exist");
        try {
            client.getBucketWebsite(nonexistentBucket);
            Assert.fail("Get bucket website should not be successful");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.NO_SUCH_BUCKET, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(NO_SUCH_BUCKET_ERR));
        }
        
        // Get bucket without ownership
        final String bucketWithoutOwnership = "oss";
        try {
            client.getBucketLogging(bucketWithoutOwnership);
            Assert.fail("Get bucket website should not be successful");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.ACCESS_DENIED, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(BUCKET_ACCESS_DENIED_ERR));
        }
        
        // Get bucket without setting website configuration
        final String bucketWithoutWebsiteConfiguration = 
                getBucketName("bucket-without-website-configuration");
        try {
            client.createBucket(bucketWithoutWebsiteConfiguration);
            
            client.getBucketWebsite(bucketWithoutWebsiteConfiguration);
            Assert.fail("Get bucket website should not be successful");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.NO_SUCH_WEBSITE_CONFIGURATION, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(NO_SUCH_WEBSITE_CONFIGURATION_ERR));
        } finally {
            client.deleteBucket(bucketWithoutWebsiteConfiguration);
        }
    }
    
    @Test
    public void testUnormalDeleteBucketWebsite() {
        // Delete non-existent bucket
        final String nonexistentBucket = getBucketName("non-exist");
        try {
            client.deleteBucketWebsite(nonexistentBucket);
            Assert.fail("Delete bucket website should not be successful");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.NO_SUCH_BUCKET, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(NO_SUCH_BUCKET_ERR));
        }
        
        // Delete bucket without ownership
        final String bucketWithoutOwnership = "oss";
        try {
            client.deleteBucketWebsite(bucketWithoutOwnership);
            Assert.fail("Delete bucket website should not be successful");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.ACCESS_DENIED, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(BUCKET_ACCESS_DENIED_ERR));
        }
        
        // Delete bucket without setting website configuration
        final String bucketWithoutWebsiteConfiguration =
                getBucketName("bucket-without-website-configuration");
        try {
            client.createBucket(bucketWithoutWebsiteConfiguration);
            client.deleteBucketWebsite(bucketWithoutWebsiteConfiguration);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            client.deleteBucket(bucketWithoutWebsiteConfiguration);
        }
    }
}
