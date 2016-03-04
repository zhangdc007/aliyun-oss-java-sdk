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
import com.aliyun.oss.model.Bucket;
import com.aliyun.oss.model.CreateBucketRequest;
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.aliyun.oss.integrationtests.TestConfig.OSS_TEST_REGION;
import static com.aliyun.oss.integrationtests.TestConstants.*;
import static com.aliyun.oss.model.LocationConstraint.OSS_CN_SHENZHEN;

public class CreateBucketTest extends TestBase {
    
    private static final int MAX_BUCKETS_ALLOWED = 10;

    @Test
    public void testPutWithDefaultLocation() {
        final String bucketName = getBucketName("bucket-with-default-location");
        
        try {
            client.createBucket(bucketName);
            String loc = client.getBucketLocation(bucketName);
            Assert.assertEquals(OSS_TEST_REGION, loc);
            
            // Create bucket with the same name again.
            client.createBucket(bucketName);
            loc = client.getBucketLocation(bucketName);
            Assert.assertEquals(OSS_TEST_REGION, loc);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            client.deleteBucket(bucketName);
        }
    }
    
    @Test
    public void testPutWithUnsupportedLocation() {
        final String bucketName = getBucketName("bucket-with-unsupported-location");
        final String unsupportedLocation = "oss-cn-zhengzhou";
        
        CreateBucketRequest request = new CreateBucketRequest(bucketName);
        request.setLocationConstraint(unsupportedLocation);
        try {
            client.createBucket(request);
            tryDeleteBucket(bucketName);
            Assert.fail("Create bucket should not be successful.");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.INVALID_LOCATION_CONSTRAINT, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(INVALID_LOCATION_CONSTRAINT_ERR));
        }
    }
    
    @Test
    public void testPutWithInconsistentLocation() {
        final String bucketName = getBucketName("bucket-with-inconsistent-location");
        
        CreateBucketRequest request = new CreateBucketRequest(bucketName);
        // Make location constraint inconsistent with endpoint 
        request.setLocationConstraint(OSS_CN_SHENZHEN);
        try {
            client.createBucket(request);
            tryDeleteBucket(bucketName);
            Assert.fail("Create bucket should not be successful.");
        } catch (OSSException e) {
            //TODO: Inconsistent with OSS API, why not IllegalLocationConstraintException(400)?
            Assert.assertEquals(OSSErrorCode.INVALID_LOCATION_CONSTRAINT, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(INVALID_LOCATION_CONSTRAINT_ERR));
        }
    }
    
    @Test
    public void testModifyExistingBucketLocation() {
        final String bucketName = getBucketName("modify-existing-bucket-location");
        
        try {
            client.createBucket(bucketName);
            
            // Try to modify location of existing bucket
            CreateBucketRequest request = new CreateBucketRequest(bucketName);
            request.setLocationConstraint(OSS_CN_SHENZHEN);
            client.createBucket(request);
            tryDeleteBucket(bucketName);
            Assert.fail("Create bucket should not be successful.");
        } catch (OSSException e) {
            //TODO: Inconsistent with OSS API, why not Conflict(409)?
            Assert.assertEquals(OSSErrorCode.INVALID_LOCATION_CONSTRAINT, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(INVALID_LOCATION_CONSTRAINT_ERR));
        }
    }
    
    @Test
    public void testPutExistingBucketWithoutOwnership() {
        final String bucketWithoutOwnership = "oss";
        
        try {
            client.createBucket(bucketWithoutOwnership);
            Assert.fail("Create bucket should not be successful.");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.BUCKET_ALREADY_EXISTS, e.getErrorCode());
            Assert.assertTrue(e.getMessage().startsWith(BUCKET_ALREADY_EXIST_ERR));
        }
    }
    
    @Test
    public void testInvalidBucketNames() {
        String[] invalidBucketNames = { "ab", "abcdefjhijklmnopqrstuvwxyz0123456789abcdefjhijklmnopqrstuvwxyz-a",
                "abC", "abc#", "-abc", "#abc", "-abc-", "Abcdefg", "abcdefg-" };
        
        for (String value : invalidBucketNames) {
            boolean created = false;
            try {
                client.createBucket(value);
                created = true;
                Assert.fail(String.format("Invalid bucket name %s should not be created successfully.", value));
            } catch (Exception ex) {
                Assert.assertTrue(ex instanceof IllegalArgumentException);
            } finally {
                if (created) {
                    client.deleteBucket(value);
                }
            }
        }
    }
    
    @Ignore
    public void testPutTooManyBuckets() {        
        final String bucketNamePrefix = "too-many-buckets-";
        
        try {
            List<String> existingBuckets = new ArrayList<String>();
            List<Bucket> bucketListing = client.listBuckets();
            for (Bucket bkt : bucketListing) {
                existingBuckets.add(bkt.getName());
            }
            
            int remaindingAllowed = MAX_BUCKETS_ALLOWED - existingBuckets.size();            
            List<String> newlyBuckets = new ArrayList<String>();
            int i = 0;
            while (i < remaindingAllowed) {
                String bucketName = bucketNamePrefix + i;
                try {
                    client.createBucket(bucketName);
                    newlyBuckets.add(bucketName);
                    i++;
                    
                    String loc = client.getBucketLocation(bucketName);
                    Assert.assertEquals(OSS_TEST_REGION, loc);
                    
                    Thread.sleep(50);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    continue;
                }
            }
            
            // Try to create (MAX_BUCKETS_ALLOWED +1)th bucket
            try {
                client.createBucket(bucketNamePrefix + MAX_BUCKETS_ALLOWED);
                Assert.fail("Create bucket should not be successful.");
            } catch (OSSException oe) {
                Assert.assertEquals(OSSErrorCode.TOO_MANY_BUCKETS, oe.getErrorCode());
                Assert.assertTrue(oe.getMessage().startsWith(TOO_MANY_BUCKETS_ERR));
            } finally {
                for (String bkt : newlyBuckets) {
                    try {
                        client.deleteBucket(bkt);
                    } catch (Exception e) {
                        // Ignore the exception and continue to delete remainding undesired buckets
                    }
                }
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}
