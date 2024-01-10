package com.nicmora.exporter.csv.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class S3Config {

    private final AwsCredentialsProvider credentialsProvider;

    public S3Config(AwsCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }

    @Bean
    public S3Client s3Client(@Value("${aws.exporter.region}") String awsRegion) {
        return S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(awsRegion))
                .build();
    }

}
