package com.example.A2;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.util.ArrayList;

public class S3Service {
    AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withRegion(Regions.US_EAST_1)
            .build();
    S3Client s3Client = S3Client.builder()
            .region(Region.US_EAST_1)
            .build();

    public String createS3Bucket(String data, String bucketName, String filePath, String fileObjKeyName) {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter writer = new FileWriter(file);
            writer.write(data);
            writer.close();

            S3Waiter s3Waiter = s3Client.waiter();

            CreateBucketRequest request = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3Client.createBucket(request);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);

            System.out.println("Bucket Created!\n");
            setPermissions(bucketName);
            setPolicy(bucketName);

            putObject(bucketName, fileObjKeyName, filePath);

            S3Utilities utilities = s3Client.utilities();
            GetUrlRequest req = GetUrlRequest.builder().bucket(bucketName).key(fileObjKeyName).build();
            URL url = utilities.getUrl(req);

            return url.toString();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    protected void putObject(String bucketName, String fileObjKeyName, String filePath) {
        try {
            PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(filePath));
            s3.putObject(request);
            System.out.println("File Uploaded!\n");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    protected void setPermissions(String bucketName) {
        try {
            s3Client.putPublicAccessBlock(
                    PutPublicAccessBlockRequest.builder()
                            .publicAccessBlockConfiguration(
                                    PublicAccessBlockConfiguration.builder().blockPublicPolicy(false).build()
                            ).bucket(bucketName).build()
            );
            System.out.println("Public Access granted!\n");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    protected void setPolicy(String bucketName) {
        PutBucketPolicyRequest policyReq = PutBucketPolicyRequest.builder()
                .bucket(bucketName)
                .policy("{\n" +
                        "    \"Version\": \"2012-10-17\",\n" +
                        "    \"Statement\": [\n" +
                        "        {\n" +
                        "            \"Sid\": \"PublicReadGetObject\",\n" +
                        "            \"Effect\": \"Allow\",\n" +
                        "            \"Principal\": \"*\",\n" +
                        "            \"Action\": \"s3:GetObject\",\n" +
                        "            \"Resource\": \"arn:aws:s3:::" + bucketName + "/*\"\n" +
                        "        }\n" +
                        "    ]\n" +
                        "}")
                .build();

        s3Client.putBucketPolicy(policyReq);

    }

    protected void deleteS3Data(String objectKey, String bucketName) {
        try {
            ArrayList<ObjectIdentifier> toDelete = new ArrayList<>();
            toDelete.add(ObjectIdentifier.builder()
                    .key(objectKey)
                    .build());

            DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder()
                            .objects(toDelete).build())
                    .build();

            s3Client.deleteObjects(dor);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    protected void deleteBucket(String bucketName) {
        try {
            DeleteBucketRequest request = DeleteBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            s3Client.deleteBucket(request);
        }
        catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

}
