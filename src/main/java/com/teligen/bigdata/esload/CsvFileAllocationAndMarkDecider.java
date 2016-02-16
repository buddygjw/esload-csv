package com.teligen.bigdata.esload;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.common.joda.time.DateTime;

import java.io.File;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by root on 2015/6/24.
 */
public class CsvFileAllocationAndMarkDecider {

    private List<CsvFileDescription> csvFileDescriptions = new ArrayList<CsvFileDescription>();
    private String poll;

    public CsvFileAllocationAndMarkDecider(){
        this.poll = Configurations.configure().getString("csv.poll");
        this.init();
    }

    /**
     * apply for a new csv file.
     * @return csv file absolutely path
     */
    public CsvFileDescription applyNewCsvFile() {
        CsvFileDescription fileDescription = null;
        synchronized(this){
            fileDescription =  csvFileDescriptions.remove(0);
            if(fileDescription == null){
                try {
                    Thread.sleep(DateTimeUtils.parseHumanDTToMills(this.poll));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                this.init();
            }else {
                String path = this.markProcessing(fileDescription.getFilePath());
                fileDescription.setFilePath(path);
            }
        }
        return fileDescription;
    }

    public void restoreProcessedFile(CsvFileDescription description) {
        this.markProcesed(description.getFilePath());
    }

    private String  markProcessing(String filePath){
        String newPath = filePath + ".processing";
        new File(filePath).renameTo(new File(newPath));
        return newPath;
    }

    private String  markProcesed(String filePath){
        String newPath = filePath + ".processed";
        new File(filePath).renameTo(new File(newPath));
        return newPath;
    }

    public void init(){
        Configuration configuration = Configurations.configure();
        String[] folders =configuration.getStringArray("csv.folder");
        String extionsions = configuration.getString("csv.filename.extensions");
        char separator = configuration.getString("csv.field.separator").charAt(0);
        String charSet = configuration.getString("csv.charset");

        for(String folder : folders){
            Collection<File> fileList = FileUtils.listFiles(new File(folder),new String[]{extionsions},false);
            int sortValue = 1;
            for(File file : fileList){
                CsvFileDescription csvFileDescription = new CsvFileDescription();
                csvFileDescription.setFilePath(file.getAbsolutePath());
                csvFileDescription.setFieldSeparator(separator);
                csvFileDescription.setFileCharset(charSet);
                csvFileDescription.setFolder(folder);
                csvFileDescription.setSortValue(String.valueOf(sortValue++));
                csvFileDescriptions.add(csvFileDescription);
            }
        }
        Collections.sort(csvFileDescriptions);
    }
}
