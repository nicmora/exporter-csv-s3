package com.nicmora.exporter.csv.mapper;

public interface CsvMapper<E> {

    String[] getRecord(E element);

}
