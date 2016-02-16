package com.teligen.bigdata.esload;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by root on 2015/6/24.
 */
public class CsvFileDescription implements Comparable<CsvFileDescription> {
    private String folder;
    private String filePath;
    private char fieldSeparator;
    private String fileCharset;
    private String sortValue;

    public String getFolder() {
        return folder;
    }

    public void setFolder(String folder) {
        this.folder = folder;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public char getFieldSeparator() {
        return fieldSeparator;
    }

    public void setFieldSeparator(char fieldSeparator) {
        this.fieldSeparator = fieldSeparator;
    }

    public String getFileCharset() {
        return fileCharset;
    }

    public void setFileCharset(String fileCharset) {
        this.fileCharset = fileCharset;
    }

    public String getSortValue() {
        return sortValue;
    }

    public void setSortValue(String sortValue) {
        this.sortValue = sortValue;
    }

    @Override
    public int compareTo(CsvFileDescription o) {
        return this.getSortValue().compareTo(o.getSortValue());
    }


    public static void main(String[] args) {
        CsvFileDescription csv1 = new CsvFileDescription();
        csv1.setSortValue("1000");
        CsvFileDescription csv2 = new CsvFileDescription();
        csv2.setSortValue("11");
        CsvFileDescription csv3 = new CsvFileDescription();
        csv3.setSortValue("100");

        List<CsvFileDescription> list = new ArrayList<CsvFileDescription>();
        list.add(csv1);
        list.add(csv2);
        list.add(csv3);

        Collections.sort(list);

        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i).getSortValue());
        }
    }
}
