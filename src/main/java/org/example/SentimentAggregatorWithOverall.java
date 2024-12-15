package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SentimentAggregatorWithOverall {

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
        sourceProps.setProperty("pulsar.consumer.subscriptionName", "sub-aggregator");
        sourceProps.setProperty("pulsar.consumer.receiverQueueSize", "100");

        PulsarSource<String> pulsarSource = PulsarSource.builder()
                .setServiceUrl("pulsar://localhost:6650")
                .setAdminUrl("http://localhost:8088")
                .setTopics("persistent://public/default/intermediate-results")
                .setProperties(sourceProps)
                .setStartCursor(StartCursor.latest())
                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema()))
                .build();

        // Read intermediate results from Pulsar
        DataStream<String> intermediateStream = env.fromSource(pulsarSource, WatermarkStrategy.noWatermarks(), "PulsarSource");

        DataStream<String> aggregatedResults = intermediateStream
                .map(result -> {
                    String[] parts = result.split(",");
                    int positiveCount = Integer.parseInt(parts[0]); // Extract Positive count
                    int negativeCount = Integer.parseInt(parts[1]); // Extract Negative count
                    return new Tuple3<>("Sentiment", positiveCount, negativeCount); // Store the counts as a tuple
                })
                .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(new TypeHint<>() {
                }))
                .keyBy(tuple -> tuple.f0) // Key by sentiment (e.g., Positive, Negative)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // Tumbling window of 10 seconds
                .aggregate(new SentimentAggregator())
                .map(aggregated -> {
                    // Calculate overall sentiment
                    int positiveCount = aggregated.f0;
                    int negativeCount = aggregated.f1;
                    String overallSentiment = positiveCount > negativeCount ? "Positive" : (positiveCount < negativeCount ? "Negative" : "Neutral");

                    String currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    return String.format(
                            "Time: %s, Overall: %s, Number of positive sentiment tweets: %d, Number of negative sentiment tweets: %d",
                            currentTime, overallSentiment, positiveCount, negativeCount);
                });

        aggregatedResults.print();

        env.execute("Sentiment Aggregator with Overall");
    }

    // Custom Aggregator to compute Positive and Negative counts
    public static class SentimentAggregator implements AggregateFunction<Tuple3<String, Integer, Integer>, Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> {

        @Override
        public Tuple3<Integer, Integer, Integer> createAccumulator() {
            return new Tuple3<>(0, 0, 0); // (Positive Count, Negative Count, Total Messages)
        }

        @Override
        public Tuple3<Integer, Integer, Integer> add(Tuple3<String, Integer, Integer> value, Tuple3<Integer, Integer, Integer> accumulator) {

            accumulator.f0 += value.f1; // Add Positive Count
            accumulator.f1 += value.f2; // Add Negative Count
            accumulator.f2 += value.f1 + value.f2;
            return accumulator;
        }

        @Override
        public Tuple3<Integer, Integer, Integer> getResult(Tuple3<Integer, Integer, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple3<Integer, Integer, Integer> merge(Tuple3<Integer, Integer, Integer> a, Tuple3<Integer, Integer, Integer> b) {
            return new Tuple3<>(a.f0 + b.f0, a.f1 + b.f1, a.f2 + b.f2); // Merge counts
        }
    }
}
