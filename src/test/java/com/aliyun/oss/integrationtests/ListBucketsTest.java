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
import com.aliyun.oss.model.BucketList;
import com.aliyun.oss.model.ListBucketsRequest;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.aliyun.oss.integrationtests.TestConfig.*;
import static com.aliyun.oss.integrationtests.TestUtils.waitForCacheExpiration;

public class ListBucketsTest extends TestBase {

    @Test
    public void testNormalListBuckets() {
        final int NUM_BUCKETS = 5;
        for (int i = 0; i < NUM_BUCKETS; i ++) {
            client.createBucket(bucketName + i);
        }

        try {
            // prefix
            BucketList bkts = client.listBuckets(bucketName, null, null);
            Assert.assertEquals(NUM_BUCKETS, bkts.getBucketList().size());

            // max-keys
            bkts = client.listBuckets(bucketName, null, 3);
            Assert.assertEquals(3, bkts.getBucketList().size());
            Assert.assertEquals(bucketName + 2, bkts.getNextMarker());

            // marker
            bkts = client.listBuckets(bucketName, bucketName + 0, 1);
            Assert.assertEquals(1, bkts.getBucketList().size());
            Assert.assertEquals(bucketName + 1, bkts.getBucketList().get(0).getName());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            for (int i = 0; i < NUM_BUCKETS; i ++) {
                tryDeleteBucket(bucketName + i);
            }
        }
    }

    @Test
    public void testUnormalListBuckets() {
        final String nonexistentBucketNamePrefix = getBucketName("nonexistent-bucket-name-prefix-");
        
        try {            
            // List all existing buckets prefix with 'nonexistent-bucket-name-prefix-'
            BucketList bucketList = client.listBuckets(nonexistentBucketNamePrefix, null, null);
            Assert.assertEquals(0, bucketList.getBucketList().size());
            
            // Set 'max-keys' equal zero(MUST be between 1 and 1000)
            client.listBuckets(null, null, 0);
            Assert.fail("List bucket should not be successful");
        } catch (OSSException e) {
            Assert.assertEquals(OSSErrorCode.INVALID_ARGUMENT, e.getErrorCode());
        } 
    }
    
}
