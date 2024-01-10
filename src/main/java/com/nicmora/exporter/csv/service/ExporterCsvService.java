package com.nicmora.exporter.csv.service;

import com.nicmora.exporter.csv.exception.DeleteFileException;
import com.nicmora.exporter.csv.exception.WriteCsvFileException;
import com.nicmora.exporter.csv.mapper.CsvMapper;
import com.opencsv.CSVWriter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Service
@Slf4j
public class ExporterCsvService<E> {

    private final String LOG_KEY = "> " + this.getClass().getSimpleName() + " >";

    private final String csvExtension = ".csv";
    private final String zipExtension = ".zip";
    private final Integer pageSize = 1000000;

    @Setter
    private String fileName;
    @Setter
    private String[] fileHeaders;
    @Setter
    private String bucketName;
    @Setter
    private String bucketPath;
    @Setter
    private CsvMapper<E> mapper;

    private final S3Service s3Service;

    public ExporterCsvService(S3Service s3Service) {
        this.s3Service = s3Service;
    }

    public String export(Function<Pageable, Page<E>> fetcher) {
        String url = this.getUrlIfExists();
        if (url != null) {
            return url;
        }

        Page<E> pager = fetcher.apply(PageRequest.of(0, pageSize));
        return Optional.ofNullable(pager)
                .filter(p -> !p.getContent().isEmpty())
                .map(Page::getTotalPages)
                .filter(total -> total > 1)
                .map(t -> this.exportZip(pager, fetcher))
                .orElseGet(() -> Optional.ofNullable(pager)
                        .filter(p -> !p.getContent().isEmpty())
                        .map(this::exportCsv)
                        .orElseThrow(() -> new RuntimeException("Error data empty"))); // TODO: Cambiar por otra exception
    }

    private String getUrlIfExists() {
        String csvFileName = String.format(fileName, LocalDate.now() + csvExtension);
        String csvBucketKey = String.format(bucketPath, csvFileName);
        String zipFileName = String.format(fileName, LocalDate.now() + zipExtension);
        String zipBucketKey = String.format(bucketPath, zipFileName);

        return Optional.ofNullable(s3Service.getUrlIfExists(bucketName, csvBucketKey))
                .orElseGet(() -> s3Service.getUrlIfExists(bucketName, zipBucketKey));
    }

    /*
        Export simple csv file
     */
    private String exportCsv(Page<E> pager) {
        List<String[]> data = new ArrayList<>();
        data.add(fileHeaders);
        for (E element : pager.getContent()) {
            data.add(mapper.getRecord(element));
        }

        String csvFileName = String.format(fileName, LocalDate.now() + csvExtension);
        String exportFilePath = this.createCsvFile(csvFileName, data);

        String bucketKey = String.format(bucketPath, csvFileName);
        String url = s3Service.upload(bucketName, bucketKey, exportFilePath);

        this.deleteFile(exportFilePath);

        return url;
    }

    /*
        Export zip with csv files
    */
    private String exportZip(Page<E> pager, Function<Pageable, Page<E>> fetcher) {
        List<String> csvFilePaths = new ArrayList<>();

        int pageNumber = 0;
        while (pageNumber < pager.getTotalPages()) {
            List<String[]> data = new ArrayList<>();
            data.add(fileHeaders);
            for (E element : pager.getContent()) {
                data.add(mapper.getRecord(element));
            }

            String csvFileName = String.format(fileName, LocalDate.now() + "-page" + pageNumber + csvExtension);
            String csvFilePath = this.createCsvFile(csvFileName, data);
            csvFilePaths.add(csvFilePath);

            pageNumber++;
            if (pageNumber < pager.getTotalPages()) {
                pager = fetcher.apply(PageRequest.of(pageNumber, pageSize));
            }
        }
        String zipFileName = String.format(fileName, LocalDate.now() + zipExtension);
        String exportFilePath = createZipFile(zipFileName, csvFilePaths);

        String bucketKey = String.format(bucketPath, zipFileName);
        String url = s3Service.upload(bucketName, bucketKey, exportFilePath);

        csvFilePaths.forEach(this::deleteFile);
        this.deleteFile(exportFilePath);

        return url;
    }

    private String createCsvFile(String csvFileName, List<String[]> data) {
        String csvFilePath = String.format("/tmp/%s", csvFileName);

        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFilePath))) {
            for (String[] csvRecord : data) {
                writer.writeNext(csvRecord);
            }
        } catch (IOException e) {
            log.error("{} Error to write csv file: {}", LOG_KEY, e.getMessage());
            throw new WriteCsvFileException("Error to write csv file: " + e.getMessage());
        }

        return csvFilePath;
    }

    private String createZipFile(String zipFileName, List<String> csvFilePaths) {
        String zipFilePath = String.format("/tmp/%s", zipFileName);

        try (ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(zipFilePath)))) {
            for (String csvFile : csvFilePaths) {
                String entryName = Paths.get(csvFile).getFileName().toString();
                zipOutputStream.putNextEntry(new ZipEntry(entryName));
                try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(csvFile))) {
                    byte[] buffer = new byte[1024];
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        zipOutputStream.write(buffer, 0, bytesRead);
                    }
                }
                zipOutputStream.closeEntry();
            }
        } catch (IOException e) {
            log.error("{} Error to write zip file: {}", LOG_KEY, e.getMessage());
            throw new WriteCsvFileException("Error to write zip file: " + e.getMessage());
        }

        return zipFilePath;
    }

    private void deleteFile(String fileName) {
        Path path = Paths.get(fileName);
        try {
            if (Files.exists(path)) {
                Files.delete(path);
            }
        } catch (IOException e) {
            log.error("{} Error to delete file: {}", LOG_KEY, e.getMessage());
            throw new DeleteFileException("Error to delete file");
        }
    }

}