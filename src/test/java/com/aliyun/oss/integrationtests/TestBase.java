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

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.utils.HttpUtil;
import com.aliyun.oss.model.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.aliyun.oss.integrationtests.TestConfig.*;
import static com.aliyun.oss.model.DeleteObjectsRequest.DELETE_OBJECTS_ONETIME_LIMIT;

public class TestBase {

    protected static String bucketName;
    protected static OSSClient client;

    protected static final String DEFAULT_ENCODING_TYPE = "url";
    protected static final String APPENDABLE_OBJECT_TYPE = "Appendable";

    @BeforeClass
    public static void oneTimeSetUp() {
        int uniq = 100000 + new Random().nextInt(10000);
        bucketName = OSS_TEST_BUCKET + uniq;
        bucketName = "java-sdk-test102240";
        client = new OSSClient(
                OSS_TEST_ENDPOINT, OSS_TEST_ACCESS_KEY_ID, OSS_TEST_ACCESS_KEY_SECRET);
        client.createBucket(bucketName);
    }

    @AfterClass
    public static void oneTimeCleanUp() {
        tryDeleteBucket(bucketName);
    }

    protected static String getBucketName(String name)
    {
        return bucketName + "-" + name;
    }

    protected static void tryDeleteBucket(String name)
    {
        try {
            client.deleteBucket(name);
        } catch (Exception e) {
            // print log
            try {
                abortAllMultipartUploads(client, bucketName);
                deleteBucketWithObjects(client, bucketName);
            } catch (Exception ee) {
                // print log
            }
        }
    }

    protected static void restoreClient()
    {
        client = new OSSClient(
                OSS_TEST_ENDPOINT, OSS_TEST_ACCESS_KEY_ID, OSS_TEST_ACCESS_KEY_SECRET);
    }

    protected static void deleteBucketWithObjects(OSSClient client, String bucketName) {
        if (!client.doesBucketExist(bucketName)) {
            return;
        }

        List<String> allObjects = listAllObjects(client, bucketName);
        int total = allObjects.size();
        if (total > 0) {
            int opLoops = total / DELETE_OBJECTS_ONETIME_LIMIT;
            if (total % DELETE_OBJECTS_ONETIME_LIMIT != 0) {
                opLoops++;
            }

            List<String> objectsToDel;
            for (int i = 0; i < opLoops; i++) {
                int fromIndex = i * DELETE_OBJECTS_ONETIME_LIMIT;
                int len = 0;
                if (total <= DELETE_OBJECTS_ONETIME_LIMIT) {
                    len = total;
                } else {
                    len = (i + 1 == opLoops) ? (total - fromIndex) : DELETE_OBJECTS_ONETIME_LIMIT;
                }
                objectsToDel = allObjects.subList(fromIndex, fromIndex + len);

                DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName);
                deleteObjectsRequest.setEncodingType(DEFAULT_ENCODING_TYPE);
                deleteObjectsRequest.setKeys(objectsToDel);
                client.deleteObjects(deleteObjectsRequest);
            }
        }
        client.deleteBucket(bucketName);
    }

    protected static void abortAllMultipartUploads(OSSClient client, String bucketName) {
        if (!client.doesBucketExist(bucketName)) {
            return;
        }

        String keyMarker = null;
        String uploadIdMarker = null;
        ListMultipartUploadsRequest listMultipartUploadsRequest;
        MultipartUploadListing multipartUploadListing;
        List<MultipartUpload> multipartUploads;
        do {
            listMultipartUploadsRequest = new ListMultipartUploadsRequest(bucketName);
            listMultipartUploadsRequest.setKeyMarker(keyMarker);
            listMultipartUploadsRequest.setUploadIdMarker(uploadIdMarker);

            multipartUploadListing = client.listMultipartUploads(listMultipartUploadsRequest);
            multipartUploads = multipartUploadListing.getMultipartUploads();
            for (MultipartUpload mu : multipartUploads) {
                String key = mu.getKey();
                String uploadId = mu.getUploadId();
                client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
            }

            keyMarker = multipartUploadListing.getKeyMarker();
            uploadIdMarker = multipartUploadListing.getUploadIdMarker();
        } while (multipartUploadListing != null && multipartUploadListing.isTruncated());
    }

    protected static List<String> listAllObjects(OSSClient client, String bucketName) {
        List<String> objs = new ArrayList<String>();
        ObjectListing objectListing;
        String nextMarker = null;

        do {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName, null, nextMarker, null,
                    DELETE_OBJECTS_ONETIME_LIMIT);
            listObjectsRequest.setEncodingType(DEFAULT_ENCODING_TYPE);
            objectListing = client.listObjects(listObjectsRequest);
            if (DEFAULT_ENCODING_TYPE.equals(objectListing.getEncodingType())) {
                nextMarker = HttpUtil.urlDecode(objectListing.getNextMarker(), "UTF-8");
            } else {
                nextMarker = objectListing.getNextMarker();
            }

            List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
            for (OSSObjectSummary s : sums) {
                if (DEFAULT_ENCODING_TYPE.equals(objectListing.getEncodingType())) {
                    objs.add(HttpUtil.urlDecode(s.getKey(), "UTF-8"));
                } else {
                    objs.add(s.getKey());
                }
            }
        } while (objectListing.isTruncated());

        return objs;
    }
}
