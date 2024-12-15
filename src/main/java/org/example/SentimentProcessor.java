package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SentimentProcessor {

    public static void main(String[] args) throws Exception {
        // Flink Stream Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(
                3,
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));

        // Pulsar Source Configuration
        Properties sourceProps = new Properties();
        sourceProps.setProperty("pulsar.consumer.subscriptionName", "sub-processor");
        sourceProps.setProperty("pulsar.consumer.receiverQueueSize", "100");

        PulsarSource<String> pulsarSource = PulsarSource.builder()
                .setServiceUrl("pulsar://localhost:6650")
                .setAdminUrl("http://localhost:8088")
                .setTopics("persistent://public/default/bitcoin-tweets")
                .setProperties(sourceProps)
                .setStartCursor(StartCursor.earliest())
                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                .build();

        // Read from Pulsar
        DataStream<String> sourceStream = env.fromSource(pulsarSource, WatermarkStrategy.noWatermarks(), "PulsarSource");

        // 1. Process for Pulsar: Calculate Sentiment Summary
        SingleOutputStreamOperator<String> sentimentSummary = sourceStream
                .map(tweet -> {
                    // Assuming the tweet contains a Sentiment field
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jsonNode = mapper.readTree(tweet);
                    return jsonNode.get("Sentiment").asText();
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .aggregate(new AggregateFunction<String, SentimentAggregate, String>() {
                    @Override
                    public SentimentAggregate createAccumulator() {
                        return new SentimentAggregate();
                    }

                    @Override
                    public SentimentAggregate add(String value, SentimentAggregate accumulator) {
                        if ("Positive".equals(value)) {
                            accumulator.positiveCount++;
                        } else if ("Negative".equals(value)) {
                            accumulator.negativeCount++;
                        }
                        return accumulator;
                    }

                    @Override
                    public String getResult(SentimentAggregate accumulator) {
                        return String.format("%d,%d",
                                accumulator.positiveCount, accumulator.negativeCount);
                    }

                    @Override
                    public SentimentAggregate merge(SentimentAggregate a, SentimentAggregate b) {
                        a.positiveCount += b.positiveCount;
                        a.negativeCount += b.negativeCount;
                        return a;
                    }
                });

        // Configure the Pulsar Sink for sentiment summary
        Properties sinkProps = new Properties();
        sinkProps.setProperty("pulsar.producer.topicName", "persistent://public/default/intermediate-results");

        PulsarSink<String> pulsarSink = PulsarSink.builder()
                .setServiceUrl("pulsar://localhost:6650")
                .setAdminUrl("http://localhost:8088")
                .setTopics("persistent://public/default/intermediate-results")
                .setSerializationSchema(PulsarSerializationSchema.flinkSchema(new SimpleStringSchema()))
                .setProperties(sinkProps)
                .build();
        sentimentSummary.sinkTo(pulsarSink);

        // 2. Process for Elasticsearch: Write raw tweets

        ElasticsearchSink<String> esSink = new Elasticsearch7SinkBuilder<String>()
                .setBulkFlushMaxActions(1)
                .setHosts(new HttpHost("localhost", 9200, "http"))
                .setEmitter((element, context, indexer) -> {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jsonNode;
                    try {
                        jsonNode = mapper.readTree(element);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }

                    // Extract fields from the raw message
                    String date = jsonNode.get("Date").asText();
                    String comment = jsonNode.get("tweet").asText();
                    String sentiment = jsonNode.get("Sentiment").asText();

                    // Add to Elasticsearch
                    String json = mapper.createObjectNode()
                            .put("Date", date)
                            .put("tweet", comment)
                            .put("Sentiment", sentiment)
                            .toString();

                    indexer.add(Requests.indexRequest()
                            .index("raw-tweets")
                            .source(json, XContentType.JSON));
                })
                .build();

// Sink original tweets to
        sourceStream.sinkTo(esSink);

        // Execute the pipeline
        env.execute("Sentiment Processor");
    }

    static class SentimentAggregate {
        volatile int positiveCount = 0;
        volatile int negativeCount = 0;
    }
}
