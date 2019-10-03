package com.hansight.commons.parquet.sink;

import com.hansight.commons.parquet.Bootstrap;
import com.hansight.commons.parquet.decode.ExtendedJsonDecoder;
import com.hansight.ueba.commons.tools.elasticsearch.ElasticsearchConnection;
import com.taobao.arthas.common.AnsiLog;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.File;
import java.io.IOException;

import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;

/**
 * Created by liujia on 2019/10/2.
 */
public class EsParquetWriter implements Runnable {

    private String index;
    private String eventType;
    private String ruleName;
    private Schema schema;
    private String elasticHost;
    private int elasticPort;

    private ParquetWriter<GenericRecord> writer;

    public EsParquetWriter(String index, String ruleName, Schema schema, Bootstrap bootstrap) {
        this.index = index;
        this.ruleName = ruleName;
        this.schema = schema;
        this.eventType = bootstrap.getEventType();
        this.elasticHost = bootstrap.getElasticHost();
        this.elasticPort = bootstrap.getElasticPort();

        try {
            File file = new File(ruleName, index);
            if (file.exists()) {
                file.delete();
            }
            writer = new AvroParquetWriter<>(new Path(ruleName, index), schema);
        } catch (IOException e) {
            AnsiLog.error(e);
        }
    }

    @Override
    public void run() {
        int counter = 0;
        try (ElasticsearchConnection connection = new ElasticsearchConnection()) {
            connection.connect(elasticHost, elasticPort, "http");
            SearchResponse searchResponse = connection.client().search(new SearchRequest(index)
                    .types(eventType)
                    .source(new SearchSourceBuilder()
                            .query(QueryBuilders.boolQuery().must(QueryBuilders.matchPhraseQuery("rule_name", ruleName)))
                            .size(10000))
                    .scroll(TimeValue.timeValueMinutes(3)));
            while (searchResponse.getHits().getHits().length > 0) {
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    try {
                        Decoder decoder = new ExtendedJsonDecoder(schema, hit.getSourceAsString());
                        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
                        writer.write(datumReader.read(null, decoder));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    counter++;
                }
                searchResponse = connection.client().searchScroll(new SearchScrollRequest(searchResponse.getScrollId())
                        .scroll(new Scroll(TimeValue.timeValueMinutes(3))));
            }
        } catch (IOException e) {
            AnsiLog.error("error while query from es: {}, index: {}", elasticHost+":"+elasticPort, index);
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                AnsiLog.error("parquet writer can't close, maybe data loss.");
            }
        }
        AnsiLog.info("data pulling for index {} has been completed, total record: {}", index, counter);
    }
}
