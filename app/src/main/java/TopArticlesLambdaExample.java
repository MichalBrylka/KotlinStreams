import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;

/**
 * In this example, we count the TopN articles from a stream of page views (aka clickstreams) that
 * reads from a topic named "PageViews". We filter the PageViews stream so that we only consider
 * pages of type article, and then map the record key to effectively nullify the user, such that
 * the we can count page views by (page, industry). The counts per (page, industry) are then
 * grouped by industry and aggregated into a PriorityQueue with descending order. Finally, we
 * perform a mapValues to fetch the top 100 articles per industry.
 * <p>
 * Note: The generic Avro binding is used for serialization/deserialization.  This means the
 * appropriate Avro schema files must be provided for each of the "intermediate" Avro classes, i.e.
 * whenever new types of Avro objects (in the form of GenericRecord) are being passed between
 * processing steps.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input/intermediate/output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic PageViews \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic TopNewsPerIndustry \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1

 */
public class TopArticlesLambdaExample {

    static final String TOP_NEWS_PER_INDUSTRY_TOPIC = "TopNewsPerIndustry";
    static final String PAGE_VIEWS = "PageViews";
    static final Duration windowSize = Duration.ofHours(1);

    private static boolean isArticle(final GenericRecord record) {
        final Utf8 flags = (Utf8) record.get("flags");
        return flags != null && flags.toString().contains("ARTICLE");
    }


    static KafkaStreams buildTopArticlesStream(final String bootstrapServers,
                                               final String schemaRegistryUrl,
                                               final String stateDir) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "top-articles-lambda-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "top-articles-lambda-example-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Serde<String> stringSerde = Serdes.String();

        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        final Serde<GenericRecord> keyAvroSerde = new GenericAvroSerde();
        keyAvroSerde.configure(serdeConfig, true);

        final Serde<GenericRecord> valueAvroSerde = new GenericAvroSerde();
        valueAvroSerde.configure(serdeConfig, false);

        final Serde<Windowed<String>> windowedStringSerde =
                WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize.toMillis());

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<byte[], GenericRecord> views = builder.stream(PAGE_VIEWS);

        final Schema schema = new Schema.Parser().parse("""
                {"namespace": "io.confluent.examples.streams.avro",
                 "type": "record",
                 "name": "PageViewStats",
                 "fields": [
                     {"name": "user", "type": "string"},
                     {"name": "page", "type": "string"},
                     {"name": "industry", "type": "string"},
                     {"name": "count", "type": "long"}
                 ]
                }""");

        final KStream<GenericRecord, GenericRecord> articleViews = views
                // filter only article pages
                .filter((dummy, record) -> isArticle(record))
                // map <page id, industry> as key by making user the same for each record
                .map((dummy, article) -> {
                    final GenericRecord clone = new GenericData.Record(article.getSchema());
                    clone.put("user", "user");
                    clone.put("page", article.get("page"));
                    clone.put("industry", article.get("industry"));
                    return new KeyValue<>(clone, clone);
                });

        final KTable<Windowed<GenericRecord>, Long> viewCounts = articleViews
                // count the clicks per hour, using tumbling windows with a size of one hour
                .groupByKey(Grouped.with(keyAvroSerde, valueAvroSerde))
                .windowedBy(TimeWindows.of(windowSize))
                .count();

        final Comparator<GenericRecord> comparator =
                (o1, o2) -> (int) ((Long) o2.get("count") - (Long) o1.get("count"));

        final KTable<Windowed<String>, PriorityQueue<GenericRecord>> allViewCounts = viewCounts
                .groupBy(
                        // the selector
                        (windowedArticle, count) -> {
                            // project on the industry field for key
                            final Windowed<String> windowedIndustry =
                                    new Windowed<>(windowedArticle.key().get("industry").toString(),
                                            windowedArticle.window());
                            // add the page into the value
                            final GenericRecord viewStats = new GenericData.Record(schema);
                            viewStats.put("page", windowedArticle.key().get("page"));
                            viewStats.put("user", "user");
                            viewStats.put("industry", windowedArticle.key().get("industry"));
                            viewStats.put("count", count);
                            return new KeyValue<>(windowedIndustry, viewStats);
                        },
                        Grouped.with(windowedStringSerde, valueAvroSerde)
                ).aggregate(
                        // the initializer
                        () -> new PriorityQueue<>(comparator),

                        // the "add" aggregator
                        (windowedIndustry, record, queue) -> {
                            queue.add(record);
                            return queue;
                        },

                        // the "remove" aggregator
                        (windowedIndustry, record, queue) -> {
                            queue.remove(record);
                            return queue;
                        },

                        Materialized.with(windowedStringSerde, new PriorityQueueSerde<>(comparator, valueAvroSerde))
                );

        final int topN = 100;
        final KTable<Windowed<String>, String> topViewCounts = allViewCounts
                .mapValues(queue -> {
                    final StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < topN; i++) {
                        final GenericRecord record = queue.poll();
                        if (record == null) {
                            break;
                        }
                        sb.append(record.get("page").toString());
                        sb.append("\n");
                    }
                    return sb.toString();
                });

        topViewCounts.toStream().to(TOP_NEWS_PER_INDUSTRY_TOPIC, Produced.with(windowedStringSerde, stringSerde));
        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

}
