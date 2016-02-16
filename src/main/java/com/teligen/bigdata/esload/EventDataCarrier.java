package com.teligen.bigdata.esload;

import com.lmax.disruptor.RingBuffer;
import com.teligen.bigdata.esload.csv.CsvReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created by root on 2015/6/24.
 */
public class EventDataCarrier implements Runnable {

    private CsvFileAllocationAndMarkDecider decider = null;
    private RingBuffer<ZCEvent> ringBuffer = null;
    private int batchSize = 10;
    private volatile boolean isNonProcess = true;
    private CsvReader csvReader = null;
    private CsvFileDescription currentFileDescription = null;

    public EventDataCarrier(CsvFileAllocationAndMarkDecider decider) {
        this.decider = decider;
    }

    public void setRingBuffer(RingBuffer<ZCEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void createNewCsvReader() {
        currentFileDescription = decider.applyNewCsvFile();
        try {
            csvReader = new CsvReader(currentFileDescription.getFilePath(), currentFileDescription.getFieldSeparator(), Charset.forName(currentFileDescription.getFileCharset()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.isNonProcess = false;
    }

    public void endCurrentCsvReader() {
        csvReader.close();
        csvReader = null;
        isNonProcess = true;
        decider.restoreProcessedFile(currentFileDescription);
    }

    @Override
    public void run() {

        while (true) {
            if (isNonProcess) {
                this.createNewCsvReader();
            }
            long hi = ringBuffer.next(batchSize);
            long lo = hi - (batchSize - 1);
            for (long l = lo; l <= hi; l++) {
                ZCEvent event = ringBuffer.get(l);
                try {
                    if (csvReader.readRecord()) {
                        event.setEventdate(csvReader.get(0));
                        event.setInta(csvReader.get(1));
                        event.setIntb(csvReader.get(2));
                        event.setShortinta(csvReader.get(3));
                        event.setShortintb(csvReader.get(4));
                        event.setChara(csvReader.get(5));
                        event.setCharb(csvReader.get(6));
                        event.setCharc(csvReader.get(7));
                        event.setBooleana(csvReader.get(8));
                        event.setChard(csvReader.get(9));
                        event.setChare(csvReader.get(10));
                        event.setBooleanb(csvReader.get(11));
                        event.setBooleanc(csvReader.get(12));
                        event.setCharf(csvReader.get(13));
                    } else {
                        this.endCurrentCsvReader();
                        break;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            ringBuffer.publish(lo, hi);
        }
    }
}
