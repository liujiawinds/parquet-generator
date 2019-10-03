package com.hansight.commons.parquet;

import com.google.common.base.Preconditions;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hansight.commons.parquet.sink.EsParquetWriter;
import com.hansight.ueba.commons.tools.elasticsearch.CustomAggregationBuilder;
import com.hansight.ueba.commons.tools.elasticsearch.ElasticsearchConnection;
import com.hansight.ueba.commons.tools.elasticsearch.ElasticsearchIndexUtils;
import com.hansight.ueba.commons.tools.elasticsearch.NewAggregationRequestGenerator;
import com.hansight.ueba.commons.tools.elasticsearch.request_sender.ElasticsearchRequestSender;
import com.hansight.ueba.commons.tools.elasticsearch.request_sender.ElasticsearchRequestSenderFactory;
import com.taobao.arthas.common.AnsiLog;
import com.taobao.arthas.common.UsageRender;
import com.taobao.middleware.cli.CLI;
import com.taobao.middleware.cli.CommandLine;
import com.taobao.middleware.cli.UsageMessageFormatter;
import com.taobao.middleware.cli.annotations.CLIConfigurator;
import com.taobao.middleware.cli.annotations.Description;
import com.taobao.middleware.cli.annotations.Name;
import com.taobao.middleware.cli.annotations.Option;
import com.taobao.middleware.cli.annotations.Summary;

import org.apache.avro.Schema;
import org.apache.lucene.util.NamedThreadFactory;
import org.codehaus.jackson.node.NullNode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.kitesdk.data.spi.JsonUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by liujia on 2019/10/1.
 */
@Name("parquet-generator-boot")
@Summary("Bootstrap Parquet Generator")
@Description("EXAMPLES:\n" + "  java -jar parquet-generator-jar-with-dependencies.jar --target-es 172.16.150.60:9200\n"
        + "  java -jar parquet-generator-jar-with-dependencies.jar --help\n")
public class Bootstrap {
    private static final String DEFAULT_ES_IP = "127.0.0.1";
    private static final int DEFAULT_ES_HTTP_PORT = 9200;

    private String elasticHost = DEFAULT_ES_IP;
    private int elasticPort = DEFAULT_ES_HTTP_PORT;
    private String eventType;
    private int aggDuration;
    private String eventPrefix;

    private static final ExecutorService executorService = new ThreadPoolExecutor(5, 10,
            30L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            new NamedThreadFactory("es-parquet-writer"));


    @Option(longName = "target-es")
    @Description("The target es URI, default 127.0.0.1:9200")
    public void setElasticHost(String uri) {
        String[] split = uri.split(":");
        this.elasticHost = split[0];
        this.elasticPort = Integer.valueOf(split[1]);
    }

    private static String usage(CLI cli) {
        StringBuilder usageStringBuilder = new StringBuilder();
        UsageMessageFormatter usageMessageFormatter = new UsageMessageFormatter();
        usageMessageFormatter.setOptionComparator(null);
        cli.usage(usageStringBuilder, usageMessageFormatter);
        return UsageRender.render(usageStringBuilder.toString());
    }

    public int readAggDuration() {
        AnsiLog.info("Please enter the days for which you would like to infer schema from Elasticsearch.");
        String line = new Scanner(System.in).nextLine();
        int duration = 7;
        if (line.trim().isEmpty()) {
            AnsiLog.info("No input found. Using default duration: 7 days");
        } else {
            duration = Integer.valueOf(line);
        }
        this.aggDuration = duration;
        return duration;
    }

    public String readEventPrefix() {
        AnsiLog.info("Please enter the Elasticsearch index prefix.");
        String line = new Scanner(System.in).nextLine();
        String eventPrefix = "event_";
        if (line.trim().isEmpty()) {
            AnsiLog.info("No input found. Using default index prefix: event_");
        } else {
            eventPrefix = line;
        }
        this.eventPrefix = eventPrefix;
        return eventPrefix;
    }

    public String readEventType() {
        AnsiLog.info("Please enter the Elasticsearch index event type.");
        String line = new Scanner(System.in).nextLine();
        String eventType = "event";
        if (line.trim().isEmpty()) {
            AnsiLog.info("No input found. Using default event type: event");
        } else {
            eventType = line;
        }
        this.eventType = eventType;
        return eventType;
    }

    public String getElasticHost() {
        return elasticHost;
    }

    public int getElasticPort() {
        return elasticPort;
    }

    public String getEventType() {
        return eventType;
    }

    public int getAggDuration() {
        return aggDuration;
    }

    public String getEventPrefix() {
        return eventPrefix;
    }

    public static String select(Map<String, Long> items) {
        int count = 0;
        for (Map.Entry<String, Long> entry : items.entrySet()) {
            if (count == 0) {
                System.out.println("* [" + count + "]: " + entry.getKey() + "  || doc count:" + entry.getValue());
            } else {
                System.out.println("  [" + count + "]: " + entry.getKey() + "  || dco count:" + entry.getValue());
            }
            count++;
        }

        String line = new Scanner(System.in).nextLine();
        if (line.trim().isEmpty()) {
            // get the first rule name
            return items.keySet().iterator().next();
        }

        int choice = new Scanner(line).nextInt();
        count = 0;
        for (String item : items.keySet()) {
            if (count++ == choice) {
                return item;
            }

        }
        return null;
    }

    public static void saveSchema(String ruleName, String schema) throws IOException {
        File file = new File(ruleName, "schema.asvc");
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        if (!file.exists()) {
            file.createNewFile();
        }
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write(schema);
        fileWriter.close();
    }

    public static JSONObject simpleAggregation(ElasticsearchConnection connection,
                                               String index, String type, QueryBuilder query,
                                               String field, String aggType) {
        Preconditions.checkArgument(connection != null && connection.client() != null,
                "Elasticsearch connection is not valid.");

        ElasticsearchRequestSender sender = ElasticsearchRequestSenderFactory.createSender(connection);

        NewAggregationRequestGenerator generator = new NewAggregationRequestGenerator()
                .size(0)
                .from(0)
                .aggregation(new CustomAggregationBuilder()
                        .setAggName("agg")
                        .setAggType(aggType)
                        .setFieldName(field)
                        .setSize(10)
                        .complete());

        sender.aggregation(generator).query(query);

        String endpoint = String.format("/%s/%s/_search", index, type);

        return sender.performRequest("POST", endpoint, Collections.EMPTY_MAP, sender.getRequestEntity());
    }

    public static Schema enableDefaultValue(Schema source) {
        List<Schema.Field> fields = new ArrayList<>();

        for (Schema.Field f : source.getFields()) {
            Schema unionType = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), f.schema()));
            Schema.Field _field = new Schema.Field(f.name(), unionType, f.doc(), NullNode.getInstance());
            fields.add(_field);
        }
        Schema result = Schema.createRecord(source.getName(), source.getDoc(), source.getNamespace(), true);
        result.setFields(fields);
        return result;
    }

    public static void main(String[] args) throws IOException {
        Bootstrap bootstrap = new Bootstrap();

        CLI cli = CLIConfigurator.define(Bootstrap.class);
        CommandLine commandLine = cli.parse(Arrays.asList(args));

        try {
            CLIConfigurator.inject(commandLine, bootstrap);
        } catch (Throwable e) {
            e.printStackTrace();
            System.out.println(usage(cli));
            System.exit(1);
        }
        if (bootstrap.elasticHost == null || DEFAULT_ES_IP.equals(bootstrap.elasticHost)) {
            AnsiLog.info("Please enter the Elasticsearch URI.");
            String line = new Scanner(System.in).nextLine();
            if (line.trim().isEmpty()) {
                AnsiLog.info("No input found. exit");
                System.exit(1);
            }
            String[] split = line.split(":");
            bootstrap.elasticHost = split[0];
            bootstrap.elasticPort = Integer.valueOf(split[1]);
        }

        int duration = bootstrap.readAggDuration();
        String indexPrefix = bootstrap.readEventPrefix();

        ElasticsearchConnection connection = new ElasticsearchConnection();
        connection.connect(bootstrap.elasticHost, bootstrap.elasticPort, "http");
        List<String> indices = ElasticsearchIndexUtils.getRecentNDaysIndices(connection, indexPrefix, duration);
        if (indices.isEmpty()) {
            AnsiLog.red("No index found in specified Elasticsearch, please check your inputs.");
            System.exit(1);
        }

        Map<String, Long> ruleNames = new HashMap<>();
        String eventType = bootstrap.readEventType();
        MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();
        JSONObject result = simpleAggregation(connection, String.join(",", indices), eventType, query, "rule_name", "terms");
        JSONObject agg = result.getJSONObject("aggregations").getJSONObject("agg");
        JSONArray buckets = agg.getJSONArray("buckets");
        for (int i = 0; i < buckets.size(); i++) {
            ruleNames.put(buckets.getJSONObject(i).get("key").toString(), Long.valueOf(buckets.getJSONObject(i).get("doc_count").toString()));
        }

        if (ruleNames.isEmpty()) {
            AnsiLog.red("No rule name found in specified Elasticsearch indices, please check your inputs.");
            System.exit(1);
        }

        AnsiLog.info("Found existing rule names, please choose one and hit RETURN.");
        String ruleName = select(ruleNames);
        // infer schema from Elasticsearch by specified rule_name
        Schema jsonSchema = null;
        for (String index : indices) {
            SearchResponse searchResponse = connection.client().search(new SearchRequest(index)
                    .types(eventType)
                    .source(new SearchSourceBuilder()
                            .query(QueryBuilders.boolQuery().must(QueryBuilders.matchPhraseQuery("rule_name", ruleName)))
                            .size(1)));
            if (searchResponse.getHits().getHits().length > 0) {
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    Schema anotherJsonSchema = JsonUtil.inferSchema(objectMapper.readTree(hit.getSourceAsString()), ruleName.replaceAll("\\-", "_"));
                    if (jsonSchema == null) {
                        jsonSchema = anotherJsonSchema;
                    } else {
                        jsonSchema = jsonSchema.getFields().size() > anotherJsonSchema.getFields().size() ? jsonSchema : anotherJsonSchema;
                    }
                }
            }
        }
        // enable null default value
        Schema finalSchema = enableDefaultValue(jsonSchema);
        AnsiLog.info("final schema: {}", finalSchema.toString());
        saveSchema(ruleName, finalSchema.toString());
        AnsiLog.info("begin to pull data ....");
        List<CompletableFuture> futures = new ArrayList<>();
        for (String index : indices) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(new EsParquetWriter(index, ruleName, finalSchema, bootstrap), executorService);
            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})).thenRun(() -> {
           AnsiLog.info("All data has been convert to Parquet file.");
        }).exceptionally(ex -> {
            ex.printStackTrace();
            return null;
        });

        System.exit(0);
    }


}
