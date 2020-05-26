package com.reader;

import com.csvreader.CsvReader;

import java.io.IOException;

public class CsvItemReader implements ItemReader {

    private CsvReader reader;

    public CsvItemReader(CsvReader reader) {
        this.reader = reader;
    }

    @Override
    public String[] nextItem() {
        try {
            if (reader.readRecord()) {
                return reader.getValues();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
