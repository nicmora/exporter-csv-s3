package com.nicmora.exporter.csv.mapper;

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
@Primary
public class DefaultCsvMapper implements CsvMapper<String> {

    @Override
    public String[] getRecord(String element) {
        return new String[0];
    }

}
