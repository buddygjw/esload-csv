package com.teligen.bigdata.esload;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by root on 2015/6/24.
 */
public class CarrierScheduler {
    private int BUFFER_SIZE = 1024 * 64;
    private RingBuffer<ZCEvent> ringBuffer = null;
    private BatchEventProcessor<ZCEvent> batchEventProcessor = null;
    private ExecutorService executor = null;
    private EventDataCarrier[] carriers = null;

    public CarrierScheduler(EventDataIndexer indexer, EventDataCarrier... carriers) {
        this.carriers = carriers;
        this.ringBuffer = RingBuffer.createMultiProducer(ZCEvent.EVENT_FACTORY, BUFFER_SIZE, new BusySpinWaitStrategy());
        SequenceBarrier sequenceBarrier = this.ringBuffer.newBarrier();
        this.batchEventProcessor = new BatchEventProcessor<ZCEvent>(ringBuffer, sequenceBarrier, indexer);

        if (this.carriers != null) {
            for (EventDataCarrier carrier : this.carriers) {
                carrier.setRingBuffer(this.ringBuffer);
            }
        }
        this.ringBuffer.addGatingSequences(batchEventProcessor.getSequence());
        this.executor = Executors.newFixedThreadPool(carriers.length + 1);
    }

    public void schedule() {
        int carrierCount = this.carriers.length;
        Future<?>[] futures = new Future[carrierCount];
        for (int i = 0; i < carrierCount; i++) {
            futures[i] = executor.submit(this.carriers[i]);
        }
        executor.submit(batchEventProcessor);

        for (int i = 0; i < carrierCount; i++) {
            try {
                futures[i].get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        batchEventProcessor.halt();
    }
}
