package com.teligen.bigdata.esload;

import com.lmax.disruptor.EventHandler;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by root on 2015/6/24.
 */
public class EventDataIndexer implements EventHandler<ZCEvent> {
    public static final String NAME = "";
    public static final String SHORTINTB = "shortintb";
    public static final String CHARC = "charc";
    private Logger logger = Logger.getLogger(EventDataIndexer.class);
    private volatile static TransportClient client = null;

    private int bulkSize;
    private int concurrentRequests;
    private String flushInterval;
    private volatile BulkProcessor bulkProcessor;

    public EventDataIndexer() {
        this.initConfig();
        this.initElasticCient();
        this.initESBulkProcessor();
    }

    public void initConfig() {
        Configuration configuration = Configurations.configure();
        this.bulkSize = configuration.getInt("bulk.size");
        this.concurrentRequests = configuration.getInt("bulk.concurrent.requests");
        this.flushInterval = configuration.getString("bulk.flush.interval");
    }

    public void initElasticCient() {
        Configuration configuration = Configurations.configure();
        String clusterName = configuration.getString("es.clustername");
        String[] addresArray = configuration.getStringArray("es.address");

        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        client = new TransportClient(settings);

        for (int i = 0; i < addresArray.length; i++) {
            String[] hostAndPort = addresArray[i].split(":");
            client.addTransportAddress(new InetSocketTransportAddress(hostAndPort[0], NumberUtils.toInt(hostAndPort[1])));
        }
        logger.info("connectedNodes:" + client.connectedNodes());
    }


    public void initESBulkProcessor() {
        this.bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                logger.info("Going to execute new bulk composed of " + request.numberOfActions() + " actions");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                logger.info("Executed bulk composed of " + request.numberOfActions() + " actions");
                if (response.hasFailures()) {
                    logger.warn("There was failures while executing bulk" + response.buildFailureMessage());
                    for (BulkItemResponse item : response.getItems()) {
                        if (item.isFailed()) {
                            logger.info("Error for " + item.getIndex() + "/" + item.getType() + "/" + item.getId() + " for " + item.getOpType() + " operation: " + item.getFailureMessage());
                        }
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.warn("Error executing bulk", failure);
            }
        }).setBulkActions(bulkSize)
                .setConcurrentRequests(this.concurrentRequests)
                .setFlushInterval(TimeValue.timeValueMillis(DateTimeUtils.parseHumanDTToMills(this.flushInterval)))
                .build();
    }


    private void esIndex(String index, String type, String id, XContentBuilder xb) throws Exception {
        this.bulkProcessor.add(new IndexRequest(index, type, id).source(xb));
    }

    private void esIndex(String index, String type, XContentBuilder xb) throws Exception {
        this.bulkProcessor.add(new IndexRequest(index, type).source(xb));
    }

    private XContentBuilder build(ZCEvent event) throws IOException {
        XContentBuilder source = jsonBuilder().startObject();
        source
                .startObject()
                .field("EVENTDATE", event.getEventdate())
                .field("INTA", event.getInta())
                .field("INTB", event.getIntb())
                .field("SHORTINTA", event.getShortinta())
                .field("SHORTINTB", event.getShortintb())
                .field("CHARA", event.getChara())
                .field("CHARB", event.getCharb())
                .field("CHARC", event.getCharc())
                .field("BOOLEANA", event.getBooleana())
                .field("CHARD", event.getChard())
                .field("CHARE", event.getChare())
                .field("BOOLEANB", event.getBooleanb())
                .field("BOOLEANC", event.getBooleanc())
                .field("CHARF", event.getCharf())
                .endObject();
        return source;
    }

    private String extractIndexName(String eventDateStr) {
        return "idx_" + eventDateStr.replaceAll("\\s.*", "").replaceAll("\\/", "");
    }

    @Override
    public void onEvent(ZCEvent zcEvent, long l, boolean b) throws Exception {
        XContentBuilder xb = this.build(zcEvent);
        String indexName = this.extractIndexName(zcEvent.getEventdate());
        this.esIndex(indexName, "testdata", xb);
    }
}