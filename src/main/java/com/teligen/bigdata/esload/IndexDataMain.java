package com.teligen.bigdata.esload;

import org.apache.commons.configuration.Configuration;

/**
 * Created by root on 2015/6/24.
 */
public class IndexDataMain {
    public static void main(String[] args) {
        IndexDataMain main = new IndexDataMain();
        main.setup();
    }

    public void setup() {
        Configuration configuration = Configurations.configure();
        CsvFileAllocationAndMarkDecider decider = new CsvFileAllocationAndMarkDecider();

        int carrierTotalCount = configuration.getInt("carrier.total.count");
        EventDataCarrier[] carriers = new EventDataCarrier[carrierTotalCount];
        for (int i = 0; i < carrierTotalCount; i++) {
            EventDataCarrier carrier = new EventDataCarrier(decider);
            carriers[i] = carrier;
        }

        EventDataIndexer indexer = new EventDataIndexer();
        CarrierScheduler carrierScheduler = new CarrierScheduler(indexer, carriers);
        carrierScheduler.schedule();
    }
}
