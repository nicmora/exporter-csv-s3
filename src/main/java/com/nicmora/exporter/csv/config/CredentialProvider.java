package com.nicmora.exporter.csv.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

@Configuration
public class CredentialProvider {

    @Bean
    public AwsCredentialsProvider credentialsProvider(
            @Value("${aws.exporter.access-key}") String awsAccessKey,
            @Value("${aws.exporter.secret-key}") String awsSecretKey) {
        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(awsAccessKey, awsSecretKey);

        return StaticCredentialsProvider.create(awsCredentials);
    }

}
