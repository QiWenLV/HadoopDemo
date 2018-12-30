package com.zqw.es;


import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class TestElasticSearch {

    TransportClient client = null;

    @SuppressWarnings("unchecked")
    @Before
    public void getClient() throws UnknownHostException {
        //设置连接的集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        //获取客户端对象
        client = new PreBuiltTransportClient(settings);
        //添加连接地址
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop21"), 9300));

        // 3 打印集群名称
//        System.out.println(client.toString());
    }

    @Test
    public void createIndex_blog() {
        // 1 创建索引
        client.admin().indices().prepareCreate("blog").get();

        // 2 关闭连接
        client.close();
    }

    @Test
    public void deleteIndex() {
        //删除索引
        client.admin().indices().prepareDelete("blog").get();

        client.close();
    }


    //创建文档（String形式添加json）
    @Test
    public void createIndexByJson() {
        // 1 文档数据准备
        String json = "{" + "\"id\":\"1\"," + "\"title\":\"基于Lucene的搜索服务器\","
                + "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"" + "}";

        IndexResponse indexResponse = client.prepareIndex("blog", "article", "1").setSource(json).execute().actionGet();

        //答应结果
        System.out.println("index: " + indexResponse.getIndex());
        System.out.println("type: " + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();
    }

    //创建文档（map形式添加json）
    @Test
    public void createIndexByMap() throws Exception {
        // 1 文档数据准备
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("id", "2");
        json.put("title", "枷锁");
        json.put("content", "看片，拍片");

        // 2 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "2").setSource(json).execute().actionGet();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        client.close();
    }

    //创建文档（builder形式添加json）
    @Test
    public void createIndexByBuilder() throws Exception {
        // 1 通过es自带的帮助类，构建json数据
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", 3)
                .field("title", "温格")
                .field("content", "买药。吃药")
                .endObject();

        // 2 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "3").setSource(builder).get();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();
    }

    //单个索引查询
    @Test
    public void queryIndex() throws Exception {

        // 1 查询文档
        GetResponse response = client.prepareGet("blog", "article", "2").get();

        // 2 打印搜索的结果
        System.out.println(response.getSourceAsString());

        // 3 关闭连接
        client.close();
    }

    //多个索引查询
    @Test
    public void queryMultiIndex() {

        // 1 查询多个文档
        MultiGetResponse response = client.prepareMultiGet()
                .add("blog", "article", "1")
                .add("blog", "article", "2", "3")
                .add("blog", "article", "2")
                .get();

        // 2 遍历返回的结果
        for (MultiGetItemResponse itemResponse : response) {
            GetResponse getResponse = itemResponse.getResponse();
            // 如果获取到查询结果
            if (getResponse.isExists()) {
                String sourceAsString = getResponse.getSourceAsString();
                System.out.println(sourceAsString);
            }
        }

        // 3 关闭资源
        client.close();
    }

    //更新数据（update）
    @Test
    public void updateData() throws Throwable {

        // 1 创建更新数据的请求对象
//        UpdateRequest updateRequest = new UpdateRequest();
//        updateRequest.index("blog");
//        updateRequest.type("article");
//        updateRequest.id("3");
        UpdateRequest updateRequest = new UpdateRequest("blog", "article", "3");

        updateRequest.doc(XContentFactory.jsonBuilder().startObject()
                // 对没有的字段添加, 对已有的字段替换
                .field("title", "蒋总你好")
                .field("content", "看片，拍片，洗片，卖片")
                .field("createDate", "2017-8-22")
                .endObject());

        // 2 获取更新后的值
        UpdateResponse indexResponse = client.update(updateRequest).get();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("create:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();
    }

    //更新文档数据（upsert）
    //设置查询条件, 查找不到则添加IndexRequest内容，查找到则按照UpdateRequest更新。
    @Test
    public void testUpsert() throws Exception {
        //没有这个文档内容就创建
        IndexRequest indexRequest = new IndexRequest("blog", "article", "5");
        indexRequest.source(XContentFactory.jsonBuilder().startObject()
                .field("title", "蒋总你好5")
                .field("content", "看片，拍片，洗片，卖片，洗脚")
                .endObject());

        //有文档内容就更新
        UpdateRequest updateRequest = new UpdateRequest("blog", "article", "3");
        updateRequest.doc(XContentFactory.jsonBuilder().startObject()
                // 对没有的字段添加, 对已有的字段替换
                .field("title", "蒋总你好5")
                .field("content", "看片，拍片，洗片，卖片5")
                .field("createDate", "2017-8-22")
                .endObject());

        // 2 获取更新后的值
        UpdateResponse indexResponse = client.update(updateRequest).get();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("create:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();
    }

    //删除文档数据（prepareDelete）
    @Test
    public void deleteData() {

        // 1 删除文档数据
        DeleteResponse indexResponse = client.prepareDelete("blog", "article", "5").get();

        // 2 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("found:" + indexResponse.getResult());

        // 3 关闭连接
        client.close();
    }


    //matchAllQuery 查询所有
    @Test
    public void queryMatchAll(){
        //查询条件
        SearchResponse response = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.matchAllQuery())
                .get();

        //获取查询对象
        SearchHits hits = response.getHits();
        //获取查询结果总数
        System.out.println("查询结果总数为：" + hits.getTotalHits());

        //遍历打印文档内容
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
        client.close();
    }

    //queryStringQuery 字段分词查询，先分词再查询
    @Test
    public void query() {
        // 1 条件查询
        SearchResponse response = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("看片"))
                .get();

        // 2 打印查询结果
        SearchHits hits = response.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果总数为：" + hits.getTotalHits());


        //遍历打印文档内容
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

        // 3 关闭连接
        client.close();
    }

    //termQuery 词条查询，
    @Test
    public void termQuery() {

        // 1 第一field查询
        SearchResponse response = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.termQuery("content", "看片"))
                .get();

        // 2 打印查询结果
        SearchHits hits = response.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果总数为：" + hits.getTotalHits());

        //遍历打印文档内容
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
        // 3 关闭连接
        client.close();
    }

    // wildcardQuery 通配符查询
    // *：表示多个字符（任意的字符）
    // ？：表示单个字符
    @Test
    public void wildcardQuery() {

        // 1 通配符查询
        SearchResponse response = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.wildcardQuery("content", "*拍*"))
                .get();

        // 2 打印查询结果
        SearchHits hits = response.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果总数为：" + hits.getTotalHits());

        //遍历打印文档内容
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
        // 3 关闭连接
        client.close();
    }

    //fuzzyQuery 模糊查询
    @Test
    public void fuzzy() {

        // 1 模糊查询
        SearchResponse response = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.fuzzyQuery("title", "lucene"))
                .get();

        // 2 打印查询结果
        SearchHits hits = response.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果总数为：" + hits.getTotalHits());

        //遍历打印文档内容
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
        // 3 关闭连接
        client.close();
    }



}





