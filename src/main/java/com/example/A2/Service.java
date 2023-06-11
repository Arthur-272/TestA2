package com.example.A2;

import computeandstorage.Computeandstorage;
import io.grpc.stub.StreamObserver;
import org.lognet.springboot.grpc.GRpcService;

import java.io.File;
import java.io.FileWriter;

@GRpcService
public class Service extends computeandstorage.EC2OperationsGrpc.EC2OperationsImplBase {
    String bucketName = "a2bucket-b00929835";
    String fileObjKeyName = "index.txt";
    String filePath = "src/main/resources/static/" + fileObjKeyName;
    S3Service s3Service = new S3Service();

    @Override
    public void storeData(Computeandstorage.StoreRequest request, StreamObserver<Computeandstorage.StoreReply> responseObserver) {
        try {
            String s3Uri = s3Service.createS3Bucket(request.getData(),bucketName, filePath, fileObjKeyName);
            responseObserver.onNext(Computeandstorage.StoreReply.newBuilder().setS3Uri(s3Uri).build());
            responseObserver.onCompleted();
            System.out.println("Data uploaded!\n");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void appendData(Computeandstorage.AppendRequest request, StreamObserver<Computeandstorage.AppendReply> responseObserver) {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter writer = new FileWriter(file, true);
            writer.write(request.getData());
            writer.close();
            s3Service.putObject(bucketName, fileObjKeyName, filePath);
            responseObserver.onNext(Computeandstorage.AppendReply.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void deleteFile(Computeandstorage.DeleteRequest request, StreamObserver<Computeandstorage.DeleteReply> responseObserver) {
        try {
            s3Service.deleteS3Data(fileObjKeyName, bucketName);
            responseObserver.onNext(Computeandstorage.DeleteReply.newBuilder().build());
            responseObserver.onCompleted();

            s3Service.deleteBucket(bucketName);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
