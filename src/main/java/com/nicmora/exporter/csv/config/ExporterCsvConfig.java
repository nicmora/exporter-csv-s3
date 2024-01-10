package com.nicmora.exporter.csv.config;

import com.nicmora.exporter.csv.service.ExporterCsvService;
import com.nicmora.exporter.csv.mapper.DefaultCsvMapper;
import com.nicmora.exporter.csv.service.S3Service;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ExporterCsvConfig {

    private final S3Service s3Service;

    public ExporterCsvConfig(S3Service s3Service) {
        this.s3Service = s3Service;
    }

    @Bean
    public ExporterCsvService<String> getDeviceExporterService(
            @Value("${sdk-contextual-exporter-devices-filename}") String fileName,
            @Value("#{'${sdk-contextual-exporter-devices-headers}'.split(',')}") String[] fileHeaders,
            @Value("${aws.exporter.bucket}") String bucketName,
            @Value("${sdk-contextual-exporter-devices-path}") String bucketPath,
            DefaultCsvMapper mapper) {

        ExporterCsvService<String> exporter = new ExporterCsvService<>(s3Service);
        exporter.setFileName(fileName);
        exporter.setFileHeaders(fileHeaders);
        exporter.setBucketName(bucketName);
        exporter.setBucketPath(bucketPath);
        exporter.setMapper(mapper);

        return exporter;
    }

}
