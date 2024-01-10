package com.nicmora.exporter.csv.service;

import com.nicmora.exporter.csv.exception.UploadFileException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;

@Slf4j
@Service
public class S3Service {

    private final String LOG_KEY = "> " + this.getClass().getSimpleName() + " >";

    private final S3Client s3Client;

    public S3Service(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public String getUrlIfExists(String bucketName, String bucketKey) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(bucketKey)
                .build();

        try {
            HeadObjectResponse headObjectResponse = s3Client.headObject(headObjectRequest);
            log.info("{} File already exists on the server", LOG_KEY);
            return s3Client.utilities().getUrl(builder -> builder.bucket(bucketName).key(bucketKey)).toString();
        } catch (S3Exception e) {
            return null;
        }
    }

    public String upload(String bucketName, String bucketKey, String filePath) {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(bucketKey)
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromFile(new File(filePath)));
            log.info("{} File was uploaded successfully", LOG_KEY);

            return s3Client.utilities().getUrl(builder -> builder.bucket(bucketName).key(bucketKey)).toString();
        } catch (AwsServiceException | SdkClientException e) {
            log.error("{} Error uploading file: {}", LOG_KEY, e.getMessage());
            throw new UploadFileException("Error to upload csv file to S3 bucket");
        }
    }

}
