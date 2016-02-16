package com.teligen.bigdata.esload;

import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequestBuilder;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class ElasticAPI {

    private Logger logger = Logger.getLogger(ElasticAPI.class);

    private static TransportClient client = null;

    //test
    public static long start = 0;
    public static  long count = 0;

    @Before
    public void startup() {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "SEARCH-Cluster").build();
        client = new TransportClient(settings);
//        client = new TransportClient();
        client.addTransportAddress(new InetSocketTransportAddress("192.192.192.86", 9300));
//        client.addTransportAddress(new InetSocketTransportAddress("192.168.15.190", 9300));
//        client.addTransportAddress(new InetSocketTransportAddress("192.168.15.191", 9300));
//        client.addTransportAddress(new InetSocketTransportAddress("192.168.15.192", 9300));
        System.out.println(client.connectedNodes());

        System.out.println("---连接成功！");
    }

    @After
    public void shutdown() {
        client.close();
        System.out.println("--关闭连接！");
    }

    @Test
    public void indexDoc1() {
        try {
            long start = System.currentTimeMillis();
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("bookname", "kimchy")
                    .field("postDate", new Date())
                    .field("message", "trying out Elasticsearch")
                    .endObject();
            String json = builder.string();
            System.out.println(json);
            IndexResponse response = client.prepareIndex("manual-index", "config-mapping-book", "1")
                    .setSource(json)
                    .execute()
                    .actionGet();

            // Index name
            String _index = response.getIndex();
            // Type name
            String _type = response.getType();
            // Document ID (generated or not)
            String _id = response.getId();
            // Version (if it's the first time you index this document, you will get: 1)
            long _version = response.getVersion();

            System.out.println("_index:" + _index);
            System.out.println("_type:" + _type);
            System.out.println("_id:" + _id);
            System.out.println("_version:" + _version);

            System.out.println("----waste time:" + (System.currentTimeMillis() - start));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void indexDoc() {
        long start = System.currentTimeMillis();
        try {
            XContentBuilder builder = jsonBuilder()
                    .startObject().field("id", UUID.randomUUID().toString())
                    .field("begintime", System.currentTimeMillis())
                    .field("phonenum", StrUtil.buildNumberStr(13))
                    .field("senderaccount", StrUtil.buildNumberStr(10))
                    .field("homearea", StrUtil.buildNumberStr(5))
                    .field("receiveraccount", StrUtil.buildNumberStr(11))
                    .field("relatehomeac", StrUtil.buildNumberStr(5))
                    .field("numbertype", StrUtil.buildNumberStr(3))
                    .field("lac_nid", StrUtil.buildNumberStr(5))
                    .field("protocol", StrUtil.buildNumberStr(3))
                    .field("capture_time", StrUtil.buildNumberStr(3))
                    .field("content", "1、测试代码功能说明          该代码是用于测试对关键词匹配次数进行汇总，将其结果插入数据库表中          2、测试说明          1)该代码是适用于进行本地测试          2)测试时，要将StormDemo\\lib目录下的jar文件加入到构建路径中          3)将StormDemo目录下的keywordfilter.txt文件放到C:\\usr\\目录下，该文件为测试样例数据文件，可以修改里面的内容，但文件名不要修改     词与词间的分隔符目前是以空格为分隔符，还没有做成可配置的分隔符          4)该测试代码要测试的关键词配置在StormDemo\\ini4j-config.ini文件中的keywords配置项,里面的关键词可以自行根据keywordfilter.txt中的内容进行配置          5)要插入的数据库相关信息配置在StormDemo\\ini4j-config.ini文件中[[databaseConnectionSection]中，     里面可以配置连接的数据库url,user,password,tableName等参数          6)目前默认的数据库连接是将结果放入192.192.191.234:1521:cds 库的mda用户下的AAASTORM表中          7)测试后的结果如下：     1\thello\t3\t     2\tstorm\t1\t          8)由于时间有限，后期计划使用trident程序来时行测试，将测试文件内容分隔符使用可配置的方式，     该测试程序在我本地测试有时没有正常执行，要运行几次才能正常把结果写进数据库中，该问题原因还没找到。          9）运行的主类程序为：KeywordFilterTopology.java")
                    .endObject();
            String json = builder.string();
            IndexResponse response = client.prepareIndex("highlight-test", "mappings_type_sms").setSource(json).execute().actionGet();
            System.out.println("----waste time:" + (System.currentTimeMillis() - start) + ",id:" + response.getId());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createIndex() {
        CreateIndexRequestBuilder requestBuilder = new CreateIndexRequestBuilder(client.admin().indices(), "");
//        CreateIndexRequestBuilder requestBuilder = new CreateIndexRequestBuilder(client.admin().indices(),"es-vs-solr-no-store");


        XContentBuilder settings = null;
        try {
            settings = jsonBuilder()
                    .startObject()
                    .startObject("index").field("number_of_shards", "2").field("number_of_replicas", "0").endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }

        XContentBuilder mapper = null;
        try {
            mapper = jsonBuilder()
                    .startObject()
                    .startObject("hd")
//                                        .startObject("_source").field("enabled",false).field("compress",true).endObject()
                    .startObject("_source").field("compress", true).endObject()
//                                        .startObject("_all").field("enabled",false).endObject()
                    .startObject("properties")
                    .startObject("ID").field("type", "string").field("store", "yes").endObject()
                    .startObject("BEGINTIME").field("type", "string").field("store", "yes").endObject()
                    .startObject("PHONENUM").field("index", "not_analyzed").field("type", "string").field("store", "yes").endObject()
                    .startObject("SENDERACCOUNT").field("type", "string").field("store", "yes").endObject()
                    .startObject("HOMEAREA").field("type", "string").field("store", "yes").endObject()
                    .startObject("RECEIVERACCOUNT").field("type", "string").field("store", "yes").endObject()
                    .startObject("RELATEHOMEAC").field("type", "string").field("store", "yes").endObject()
//                                            .startObject("CONTENT"). field("type","string").field("store",false).field("index","analyzed").field("indexAnalyzer","index_ansj").field("searchAnalyzer","query_ansj").endObject()
//                                            .startObject("CONTENT"). field("type","string").field("store","yes").field("index","analyzed").field("indexAnalyzer","smartcn").field("searchAnalyzer","smartcn").endObject()
                    .startObject("CONTENT").field("type", "string").field("index", "analyzed").field("store", "false").endObject()
                    .startObject("NUMBERTYPE").field("type", "string").field("store", "yes").endObject()
                    .startObject("LAC_NID").field("type", "string").field("store", "yes").endObject()
                    .startObject("PROTOCOL").field("type", "string").field("store", "yes").endObject()
//                                            .startObject("name") 
//                                                .field("type","object")
//                                                .startObject("properties")
//                                                    .startObject("full").field("type","string").endObject()
//                                                    .startObject("first").field("type","string").endObject()
//                                                    .startObject("last").field("type","string").endObject()
//                                                .endObject()
//                                            .endObject()
                    .endObject()
                    .endObject()
                    .endObject();

            System.out.println("mapper:" + mapper.string());

            requestBuilder.setSettings(settings);
            requestBuilder.addMapping("hd", mapper);
            CreateIndexResponse response = requestBuilder.execute().actionGet();
            System.out.println("isAcknowledged:" + response.isAcknowledged());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @Test
    public void createIndexByConfig() {
        CreateIndexRequestBuilder requestBuilder = new CreateIndexRequestBuilder(client.admin().indices(), "manual-index");
        XContentBuilder settings = null;
        try {
            settings = jsonBuilder()
                    .startObject()
                    .startObject("index").field("number_of_shards", "2").field("number_of_replicas", "0").endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            requestBuilder.setSettings(settings);
            CreateIndexResponse response = requestBuilder.execute().actionGet();
            System.out.println("isAcknowledged:" + response.isAcknowledged());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createIndexByTpl() {
        CreateIndexRequestBuilder requestBuilder = new CreateIndexRequestBuilder(client.admin().indices(), "20140812-tifu");
        try {
            CreateIndexResponse response = requestBuilder.execute().actionGet();
            System.out.println("isAcknowledged:" + response.isAcknowledged());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void updateIndex() {
        UpdateSettingsRequestBuilder requestBuilder = new UpdateSettingsRequestBuilder(client.admin().indices(), "hadoop");

        XContentBuilder settings = null;
        try {
            settings = jsonBuilder()
                    .startObject()
                    .startObject("index").field("index.store.type", "memory").endObject()
//                    .startObject("index").field("number_of_replicas", "2").endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            requestBuilder.setSettings(settings.string());
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            UpdateSettingsResponse response = requestBuilder.execute().actionGet();
            System.out.println("isAcknowledged:" + response.isAcknowledged());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void updateMapping() {

        PutMappingRequestBuilder putMappingBuilder = new PutMappingRequestBuilder(client.admin().indices());
        putMappingBuilder.setIndices("hadoop-compress-test1");
        putMappingBuilder.setType("a?*&^<>b@");
        XContentBuilder mapper = null;
        try {
            mapper = jsonBuilder()
                    .startObject()
                    .startObject("a?*&^<>b@")
                    .startObject("properties")
                    .startObject("bbbbbbbbbb").field("type", "float").field("store", "false").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            putMappingBuilder.setSource(mapper);
            PutMappingResponse putResponse = putMappingBuilder.execute().actionGet();
            System.out.println("isAcknowledged:" + putResponse.isAcknowledged());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void putWarmer() {
        PutWarmerResponse response = client.admin().indices()
                .preparePutWarmer("library_warmer")
                .setSearchRequest(client.prepareSearch("library")
                        .addFacet(FacetBuilders
                                .termsFacet("tags").field("tags")))
                .execute().actionGet();
    }

    @Test
    public void get() {
        GetResponse response = client.prepareGet("twitter", "tweet", "1")
                .execute()
                .actionGet();
        String result = response.getSourceAsString();
        System.out.println("get result--" + result);

    }

    @Test
    public void deleteDoc() {
        long start = System.currentTimeMillis();
        DeleteResponse response = client.prepareDelete("", "", "")
                .execute()
                .actionGet();
        System.out.println("删除文档时间：" + (System.currentTimeMillis() - start) + "ms");
        System.out.println("delete ---" + response.getIndex());
    }

    @Test
    public void deleteAllDoc() {

        long start = System.currentTimeMillis();
        /*DeleteResponse response = client.prepareDelete("test", "tweet", "1")
                .execute()
                .actionGet();*/

        QueryBuilder qb = QueryBuilders.matchAllQuery();
        DeleteByQueryResponse response = client.prepareDeleteByQuery("es-vs-solr-50g").setQuery(qb).execute().actionGet();

        System.out.println("delete ---" + response.status().getStatus());
        System.out.println("删除所有文档时间：" + (System.currentTimeMillis() - start) + "ms");
    }

    @Test
    public void deleteIndex() {
        long start = System.currentTimeMillis();
        DeleteIndexRequest delRequest = new DeleteIndexRequest("idx_cluster4_8"); //使用_all删除所有所有索引
//        DeleteIndexRequest delRequest = new DeleteIndexRequest("es-vs-solr-10g"); //使用_all删除所有所有索引
        IndicesAdminClient indexAdminClient = client.admin().indices();
        ActionFuture<DeleteIndexResponse> deleteResponse = indexAdminClient.delete(delRequest);
        System.out.println("delete:" + deleteResponse.actionGet().toString());
        System.out.println("删除索引时间：" + (System.currentTimeMillis() - start) + "ms");

    }

    @Test
    public void bulk() throws Exception {
        BulkRequestBuilder bulkRequest = client.prepareBulk();


        // either use client#prepare, or use Requests# to directly build index/delete requests
        bulkRequest.add(client.prepareIndex("twitter", "tweet", "1")
                        .setSource(jsonBuilder()
                                        .startObject()
                                        .field("user", "kimchy")
                                        .field("postDate", new Date())
                                        .field("message", "trying out Elasticsearch")
                                        .endObject()
                        )
        );

        bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
                        .setSource(jsonBuilder()
                                        .startObject()
                                        .field("user", "kimchy")
                                        .field("postDate", new Date())
                                        .field("message", "another post")
                                        .endObject()
                        )
        );

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        } else {
            System.out.println("bulk ok");
        }
    }

    @Test
    public void search() {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("idx_sms_201601*");
//        searchRequestBuilder.setTypes("tweet");
        searchRequestBuilder.setSearchType(SearchType.SCAN);
//        searchRequestBuilder.setQuery(QueryBuilders.matchPhraseQuery("content", "日前到服务厅或合作网点"));

//        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        searchRequestBuilder.setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.queryFilter(QueryBuilders.matchPhraseQuery("content", "账户信息"))));
        searchRequestBuilder.setFrom(0);
        searchRequestBuilder.setSize(10);
        searchRequestBuilder.setScroll(new TimeValue(1000));

        System.out.println("json:" + searchRequestBuilder.toString());

//        searchRequestBuilder.setExplain(true);

        try {
            long start = System.currentTimeMillis();
            SearchResponse response = searchRequestBuilder.execute().actionGet();
            System.out.println("搜索时间：" + (System.currentTimeMillis() - start) + "ms");
            SearchHits hits = response.getHits();
            System.out.println("search result---" + hits.getTotalHits());

            for (SearchHit hit : hits.getHits()) {
                System.out.println(hit.getSourceAsString());
            }
        /*SearchResponse response = client.prepareSearch().execute().actionGet();
            SearchHits hits = response.getHits();
            System.out.println("search result---"+hits.getTotalHits());*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void constantScoreSearch() {


        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("idx_other_10");
//        searchRequestBuilder.setTypes("tweet");
//        searchRequestBuilder.setSearchType(SearchType.QUERY_AND_FETCH);
//        searchRequestBuilder.setQuery(QueryBuilders.matchPhraseQuery("content", "日前到服务厅或合作网点"));
//        searchRequestBuilder.setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.matchPhraseQuery("content", "朝花夕拾")));
//        searchRequestBuilder.setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.("content", "朝花夕拾")));
        searchRequestBuilder.setQuery(QueryBuilders.fuzzyQuery("content", "花夕"));

//        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        searchRequestBuilder.setFrom(0);
        searchRequestBuilder.setSize(50);
//        searchRequestBuilder.setExplain(true);

        try {
            long start = System.currentTimeMillis();
            SearchResponse response = searchRequestBuilder.execute().actionGet();
            System.out.println("搜索时间：" + (System.currentTimeMillis() - start) + "ms");
            SearchHits hits = response.getHits();
            System.out.println("search result---" + hits.getTotalHits());
            for (SearchHit hit : hits.getHits()) {
                System.out.println(hit.getSourceAsString());
            }
        /*SearchResponse response = client.prepareSearch().execute().actionGet();
            SearchHits hits = response.getHits();
            System.out.println("search result---"+hits.getTotalHits());*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void query() {

        long start = System.currentTimeMillis();

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("highlight-test");
//        searchRequestBuilder.setTypes("tifu");
        searchRequestBuilder.addField("content");
        searchRequestBuilder.addHighlightedField("content");
        searchRequestBuilder.setHighlighterPreTags("<font colr='red'>");
        searchRequestBuilder.setHighlighterPostTags("</font>");
        searchRequestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        searchRequestBuilder.setQuery(QueryBuilders.matchQuery("content"," 试代码功能"));

//        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

        XContentBuilder query = null;
        try {
            query = jsonBuilder()
                    .startObject()
                    .startObject("match")
                    .startObject("CONTENT")
                    .field("query", "萧炎")
                    .endObject()
                    .endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
//
//        String a = "{\n" +
//                "    \"match\": {\n" +
//                "      \"content\": \"按通话键或选项键\"\n" +
//                "    }\n" +
//                "}";
//        searchRequestBuilder.setQuery(a);
//        searchRequestBuilder.setQuery("{\"match\":{\"CONTENT\":{\"query\":\"萧炎\"}}}");
        searchRequestBuilder.setFrom(0);
        searchRequestBuilder.setSize(50);
//        searchRequestBuilder.setExplain(true);

        System.out.println("json:" + searchRequestBuilder.toString());

        try {
            SearchResponse response = searchRequestBuilder.execute().actionGet();
            SearchHits hits = response.getHits();
            System.out.println("搜索时间：" + (System.currentTimeMillis() - start) + "ms");
            System.out.println("search result---" + hits.getTotalHits());

            for (SearchHit hit : hits.getHits()) {
                System.out.println(hit.getSourceAsString());
                String[] matchedQueries = hit.getMatchedQueries();
                Map<String, HighlightField> hf = hit.getHighlightFields();
                HighlightField hfield = hf.get("content");
                System.out.println("name:"+hfield.name());
                for (Text t : hfield.getFragments()) {
                    System.out.println(t.string());
                }
            }
                
                
                /*        SearchResponse response = client.prepareSearch().execute().actionGet();
            SearchHits hits = response.getHits();
            System.out.println("search result---"+hits.getTotalHits());*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void scroll() throws Exception {
        QueryBuilder qb = QueryBuilders.matchPhraseQuery("content", "按通话键或选项键");

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("idx_cluster1_14");
        searchRequestBuilder.addField("content");
        searchRequestBuilder.addHighlightedField("content");
        searchRequestBuilder.setHighlighterPreTags("<em>");
        searchRequestBuilder.setHighlighterPostTags("</em>");
        long start = System.currentTimeMillis();
        SearchResponse scrollResp = searchRequestBuilder
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(1000))
                .setQuery(qb)
                .setFrom(0)
                .setSize(50).execute().actionGet(); //100 hits per shard will be returned for each create
        //Scroll until no hits are returned
//        while (true) {


        scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(1000)).execute().actionGet();
        long end = System.currentTimeMillis();
        System.out.println(end - start);

        StringBuffer result = new StringBuffer("");
        SearchHits hits = scrollResp.getHits();
        SearchHit[] hitss = hits.getHits();
        for (int i = 0; i < hitss.length; i++) {
            SearchHit hit = hitss[i];
            ///////////////////Field Value////////////
            result.append("|--------------------------------------------------------\n");
            result.append("[ID]:" + hit.getId()).append("\n");
            result.append("[SCORE]:" + hit.getScore()).append("\n");
            result.append("[INDEX/TYPE]:" + hit.getIndex() + "/" + hit.getType()).append("\n");
            SearchShardTarget shard = hit.getShard();
            result.append("[SHARD]:").append("index:" + shard.getIndex()).append(",shard:").append(shard.getNodeId()).append(",nodeid:").append(shard.getNodeId());
            Map<String, SearchHitField> searchHitFieldMap = hit.getFields();
            for (Map.Entry<String, SearchHitField> me : searchHitFieldMap.entrySet()) {
                String fieldName = me.getKey();
                SearchHitField searchHitField = me.getValue();
                //result.append("|" + fieldName + ":" + searchHitField.getValue()).append("\n");
            }

            /////////////////hightlight/////////////
            Map<String, HighlightField> hf = hit.getHighlightFields();
            HighlightField hfield = hf.get("content");
            if (hfield != null) {
                for (Text text : hfield.getFragments()) {
                    result.append("[Hightlight Content]:[" + text.string() + "]").append("\n");
                }
            }
            result.append("|_______________________________________________________").append("\n");
        }
        System.out.println(result);
    }


    public String emptyAndNull(Object obj) {
        String content = String.valueOf(obj);
        return content.replaceAll("\\s", "");
    }

    @Test
    public void scroll1() {
//        QueryStringQueryBuilder qb = QueryBuilders.queryStringQuery("营厅考核");
//        qb.defaultOperator(QueryStringQueryBuilder.Operator.AND);
//        qb.autoGeneratePhraseQueries(true);
//        qb.analyzer("query_ansj");

        QueryBuilder qb = QueryBuilders.matchPhraseQuery("content", "广州");

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("idx_sms_20160110");
        searchRequestBuilder.addField("content");
        searchRequestBuilder.addField("capture_time");
        searchRequestBuilder.addHighlightedField("content");
        searchRequestBuilder.setHighlighterPreTags("<em>");
        searchRequestBuilder.setHighlighterPostTags("</em>");


        start = System.currentTimeMillis();
        System.out.println("starttime:"+start);
        SearchResponse scrollResp = searchRequestBuilder
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000000))
                .setQuery(qb)
                .addSort("capture_time", SortOrder.ASC)
                .setSize(5).execute().actionGet(); //100 hits per shard will be returned for each create

        SearchHits searchHits = scrollResp.getHits();

        while (true) {
            String scrollId = scrollResp.getScrollId();
            long scrollstart = System.currentTimeMillis();
            scrollResp = client.prepareSearchScroll(scrollId).setScroll(new TimeValue(60000000)).execute().actionGet();
            long scrollend = System.currentTimeMillis();
            SearchHits searchHits1 = scrollResp.getHits();
            System.out.println("scoll-time:"+(scrollend-scrollstart)+"ms,oneQueryCount:"+count+",getTotalHits:" + searchHits.getTotalHits()+",getScrollId:" + scrollResp.getScrollId());

            //Break condition: No hits are returned
            if (searchHits1.getHits().length == 0) {
                break;
            }
        }
    }

    @Test
    public void clearScroll1() {
//         ClearScrollRequestBuilder clearScrollRequestBuilder = client.prepareClearScroll().addScrollId("c2NhbjsxOTI7MjcwNDk6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNzA1MDpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI0MDkyOl9IX2lzQW1QUW15ZDFfNHlSdnBKN3c7MjQwOTM6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNzA1MTpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI3MDU0OkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjQwOTQ6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNzA1MjpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI0MDk1Ol9IX2lzQW1QUW15ZDFfNHlSdnBKN3c7MjQwOTY6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNDA5NzpfSF9pc0FtUFFteWQxXzR5UnZwSjd3OzI0MTAwOl9IX2lzQW1QUW15ZDFfNHlSdnBKN3c7MjcwNTM6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNDA5ODpfSF9pc0FtUFFteWQxXzR5UnZwSjd3OzI3MDU1OkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjQwOTk6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNDEwMjpfSF9pc0FtUFFteWQxXzR5UnZwSjd3OzI0MTAxOl9IX2lzQW1QUW15ZDFfNHlSdnBKN3c7MjQxMDM6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNzA1NjpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI3MDU5OkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjQxMDQ6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNzA1NzpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI3MDU4OkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjQxMDU6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNDg4MTo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI3MDYwOkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjQxMDY6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNzA2MTpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI3MDYyOkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjQxMDc6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNDEwODpfSF9pc0FtUFFteWQxXzR5UnZwSjd3OzI0MTA5Ol9IX2lzQW1QUW15ZDFfNHlSdnBKN3c7MjQxMTA6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNzA2MzpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI0ODgyOjZZQ05SOWNUUTRldUFWR0YzbTlXNHc7MjcwNjQ6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNDExMTpfSF9pc0FtUFFteWQxXzR5UnZwSjd3OzI0MTEyOl9IX2lzQW1QUW15ZDFfNHlSdnBKN3c7MjQxMTM6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNzA2NTpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI0MTE0Ol9IX2lzQW1QUW15ZDFfNHlSdnBKN3c7MjcwNjY6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNzA1NDpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDY3OkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjQxMTU6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNzA2ODpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI3MDY5OkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjQ4ODM6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNDg4NDo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI3MDU1Om93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjcwNTY6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNzA1Nzpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI0MTE2Ol9IX2lzQW1QUW15ZDFfNHlSdnBKN3c7MjQxMTg6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNzA1ODpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI0ODg1OjZZQ05SOWNUUTRldUFWR0YzbTlXNHc7MjQ4ODY6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNDg4Nzo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI0MTE3Ol9IX2lzQW1QUW15ZDFfNHlSdnBKN3c7MjQ4ODg6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNzA1OTpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDYwOm93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjcwNjE6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNDg4OTo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI3MDYyOm93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjQ4OTA6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNzA2Mzpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI0ODkxOjZZQ05SOWNUUTRldUFWR0YzbTlXNHc7MjQ4OTI6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNzA2NDpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDY1Om93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjQ4OTM6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNzA2Njpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDY3Om93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjcwNjg6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNzA2OTpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI0ODk0OjZZQ05SOWNUUTRldUFWR0YzbTlXNHc7MjcwNzA6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNDg5NTo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI3MDcxOm93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjQ4OTY6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNzA3Mjpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDczOm93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjQ4OTc6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNzA3NDpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI0ODk4OjZZQ05SOWNUUTRldUFWR0YzbTlXNHc7MjcwNzU6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNzA3Njpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDc3Om93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjQ4OTk6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNzA3ODpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDc5Om93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjQ5MDA6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNDkwMTo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI3MDgwOm93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjcwODE6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNzA3MDpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI3MDcxOkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjQxMTk6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNzA4Mjpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDc0OkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjcwNzM6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNzA3MjpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI0MTIwOl9IX2lzQW1QUW15ZDFfNHlSdnBKN3c7MjcwODM6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNDEyMTpfSF9pc0FtUFFteWQxXzR5UnZwSjd3OzI3MDg0Om93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjcwNzU6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNzA3NjpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI0MTIyOl9IX2lzQW1QUW15ZDFfNHlSdnBKN3c7MjcwNzc6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNDEyMzpfSF9pc0FtUFFteWQxXzR5UnZwSjd3OzI0MTI0Ol9IX2lzQW1QUW15ZDFfNHlSdnBKN3c7MjQxMjU6X0hfaXNBbVBRbXlkMV80eVJ2cEo3dzsyNzA3ODpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI3MDg1Om93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjcwNzk6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNDEyNjpfSF9pc0FtUFFteWQxXzR5UnZwSjd3OzI0OTAyOjZZQ05SOWNUUTRldUFWR0YzbTlXNHc7MjcwODY6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNzA4MDpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI3MDg3Om93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjcwODE6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNzA4ODpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDgyOkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjcwODM6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNzA4OTpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDg0OkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjcwODU6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNzA5MDpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDg2OkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjcwODc6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNzA5MTpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDkyOm93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjcwOTM6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNzA4ODpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI3MDg5OkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjcwOTA6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNzA5NDpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDk1Om93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjcwOTY6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNzA5Nzpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MDkxOkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjcwOTg6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNzA5OTpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MTAwOm93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjQ5MDM6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNDkwNDo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI0OTA1OjZZQ05SOWNUUTRldUFWR0YzbTlXNHc7MjcxMDE6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNDkwNjo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI3MTAyOm93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjcxMDM6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNDkwNzo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI0OTA4OjZZQ05SOWNUUTRldUFWR0YzbTlXNHc7MjQ5MDk6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNDkxMDo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI0OTExOjZZQ05SOWNUUTRldUFWR0YzbTlXNHc7MjQ5MTI6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNDkxMzo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI3MTA0Om93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjcxMDU6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNDkxNDo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI3MTA2Om93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjcxMDc6b3dRTFJTVTBRcml5V0VzUWhhczhSdzsyNzEwODpvd1FMUlNVMFFyaXlXRXNRaGFzOFJ3OzI3MTA5Om93UUxSU1UwUXJpeVdFc1FoYXM4Unc7MjQ5MTU6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNDkxNjo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI0OTE3OjZZQ05SOWNUUTRldUFWR0YzbTlXNHc7MjQ5MTg6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNzA5MjpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI3MDkzOkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjQ5MTk6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNDkyMDo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI3MDk0OkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjcwOTU6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNzA5NjpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI0OTIxOjZZQ05SOWNUUTRldUFWR0YzbTlXNHc7MjcwOTc6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNDkyMjo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI3MDk4OkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjcwOTk6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNDkyMzo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI3MTAwOkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjQ5MjQ6NllDTlI5Y1RRNGV1QVZHRjNtOVc0dzsyNDkyNTo2WUNOUjljVFE0ZXVBVkdGM205VzR3OzI3MTAxOkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MjcxMDI6RXhGdGZtcEpUMnlpT3lpaWVlRFJUUTsyNzEwMzpFeEZ0Zm1wSlQyeWlPeWlpZWVEUlRROzI3MTA0OkV4RnRmbXBKVDJ5aU95aWllZURSVFE7MTt0b3RhbF9oaXRzOjE5NzE2NjI1Ow");
         ClearScrollRequestBuilder clearScrollRequestBuilder = client.prepareClearScroll().addScrollId("_all");
        ListenableActionFuture<ClearScrollResponse> responseListenableActionFuture = clearScrollRequestBuilder.execute();
        try {
            ClearScrollResponse response =  responseListenableActionFuture.get();
            System.out.println(response.isSucceeded());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void count() {
        QueryBuilder qb = QueryBuilders.matchPhraseQuery("content", "广州");
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("idx_sms_20160106");
        searchRequestBuilder.addField("content");
        searchRequestBuilder.addField("capture_time");
        searchRequestBuilder.addHighlightedField("content");
        searchRequestBuilder.setHighlighterPreTags("<em>");
        searchRequestBuilder.setHighlighterPostTags("</em>");
        long start1 = System.currentTimeMillis();
        SearchResponse scrollResp = searchRequestBuilder
//                .setSearchType(SearchType.SCAN)
//                .setScroll(new TimeValue(100000))
                .setQuery(qb)
                .setFrom(0)
                .setSize(2)
                .addSort("capture_time", SortOrder.ASC)
                .execute().actionGet(); //100 hits per shard will be returned for each create

        System.out.println("oneQueryCount:"+scrollResp.getHits().getTotalHits());
        System.out.println("costs:"+(System.currentTimeMillis()-start1));
    }

    @Test
    public void alias() {
        try {
            IndicesAliasesResponse response = client.admin().indices()
                    .prepareAliases()
                    .addAlias(new String[]{"test-tifu", "test1-tifu"}, "test12-alias")
//                .addAlias("library", "elastic_books", 
//                FilterBuilders.termFilter("title", "elasticsearch"))
//                .removeAlias("news", "current_news")
                    .execute().actionGet();
            System.out.println("response:" + response.isAcknowledged());

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testProcessBulk() {
        int count = 0;
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("Going to execute new bulk composed of {} actions" + request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("Executed bulk composed of {} actions" + request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("Error executing bulk" + failure);
            }
        }).setBulkActions(1000).setConcurrentRequests(10).build();

        while (true) {
            try {
                XContentBuilder builder = jsonBuilder()
                        .startObject().field("ID", UUID.randomUUID().toString())
                        .field("BEGINTIME", System.currentTimeMillis())
                        .field("PHONENUM", StrUtil.buildNumberStr(13))
                        .field("SENDERACCOUNT", StrUtil.buildNumberStr(10))
                        .field("HOMEAREA", StrUtil.buildNumberStr(5))
                        .field("RECEIVERACCOUNT", StrUtil.buildNumberStr(11))
                        .field("RELATEHOMEAC", StrUtil.buildNumberStr(5))
                        .field("NUMBERTYPE", StrUtil.buildNumberStr(3))
                        .field("LAC_NID", StrUtil.buildNumberStr(5))
                        .field("PROTOCOL", StrUtil.buildNumberStr(3))
                        .field("CONTENT", "    “。<a>adsfasf</a><body><table>44")
                        .endObject();
                IndexRequest indexRequest = Requests.indexRequest("zch-test1-tifu").type("SMS_MAPPINGS_TYPE");
                indexRequest.source(builder);
                bulkProcessor.add(indexRequest);
                count++;
            } catch (IOException e) {
                System.out.println("Error while importing documents from solr to elasticsearch" + e.getMessage());
                bulkProcessor.close();
            }
        }
    }

    public void testOptimize(int max_segments_num, String indexName) {

        long start = System.currentTimeMillis();
        OptimizeRequestBuilder builder = client.admin().indices().prepareOptimize(indexName);
        builder.setMaxNumSegments(max_segments_num);
        ListenableActionFuture<OptimizeResponse> responseListenableActionFuture = builder.execute();
        try {
            OptimizeResponse response = responseListenableActionFuture.get();
            System.out.println("index:" + indexName + ",segmentnum:" + max_segments_num + ",getShardFailures:" + response.getFailedShards() + ",getTotalShards:" + response.getTotalShards());
            System.out.println("costs:" + (System.currentTimeMillis() - start) + "ms");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void optimizes() {
        testOptimize(1, "idx_sms_1");
        testOptimize(3, "idx_sms_3");
        testOptimize(5, "idx_sms_5");
    }

    static class StrUtil {
        public static String buildNumberStr(int num) {
            return String.valueOf(new Random().nextLong());
        }
    }
}
