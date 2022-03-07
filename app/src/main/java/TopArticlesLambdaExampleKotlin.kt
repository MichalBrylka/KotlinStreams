import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.nio.file.Paths
import java.time.Duration
import java.util.*

/**
 * $ bin/kafka-topics --create --topic PageViews \
 * --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic TopNewsPerIndustry \
 * --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 *
 */
object TopArticlesLambdaExampleKotlin {
    const val TOP_NEWS_PER_INDUSTRY_TOPIC = "TopNewsPerIndustry"
    const val PAGE_VIEWS = "PageViews"
    val windowSize = Duration.ofHours(1)
    private fun isArticle(record: GenericRecord): Boolean {
        val flags = record["flags"] as Utf8
        return flags != null && flags.toString().contains("ARTICLE")
    }

    fun buildTopArticlesStream(
        bootstrapServers: String,
        schemaRegistryUrl: String,
        stateDir: String? = null
    ): KafkaStreams {
        val config = Properties()
        config[StreamsConfig.APPLICATION_ID_CONFIG] = "top-articles-lambda-example"
        config[StreamsConfig.CLIENT_ID_CONFIG] = "top-articles-lambda-example-client"
        config[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        config[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl
        config[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        config[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = GenericAvroSerde::class.java
        config[StreamsConfig.STATE_DIR_CONFIG] =
            if (stateDir != null && stateDir.isNotBlank()) stateDir else Paths.get(System.getProperty("java.io.tmpdir"), "kafka").toString()
        config[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = 10 * 1000

        val stringSerde = Serdes.String()
        val serdeConfig =
            Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
        val keyAvroSerde: Serde<GenericRecord> = GenericAvroSerde()
        keyAvroSerde.configure(serdeConfig, true)
        val valueAvroSerde: Serde<GenericRecord> = GenericAvroSerde()
        valueAvroSerde.configure(serdeConfig, false)
        val windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(
            String::class.java, windowSize.toMillis()
        )
        val builder = StreamsBuilder()
        val views = builder.stream<ByteArray, GenericRecord>(PAGE_VIEWS)
        val schema: Schema = Schema.Parser().parse(
            """
                {"namespace": "io.confluent.examples.streams.avro",
                 "type": "record",
                 "name": "PageViewStats",
                 "fields": [
                     {"name": "user", "type": "string"},
                     {"name": "page", "type": "string"},
                     {"name": "industry", "type": "string"},
                     {"name": "count", "type": "long"}
                 ]
                }""".trimIndent()
        )

        val articleViews = views // filter only article pages
            .filter { _: ByteArray?, record: GenericRecord -> isArticle(record) } // map <page id, industry> as key by making user the same for each record
            .map { _: ByteArray?, article: GenericRecord ->
                val clone: GenericRecord = GenericData.Record(article.schema)
                clone.put("user", "user")
                clone.put("page", article["page"])
                clone.put("industry", article["industry"])
                KeyValue(clone, clone)
            }
        val viewCounts = articleViews // count the clicks per hour, using tumbling windows with a size of one hour
            .groupByKey(Grouped.with(keyAvroSerde, valueAvroSerde))
            .windowedBy(TimeWindows.of(windowSize))
            .count()
        val comparator =
            java.util.Comparator { o1: GenericRecord, o2: GenericRecord -> (o2["count"] as Long - o1["count"] as Long).toInt() }
        val allViewCounts = viewCounts
            .groupBy( // the selector
                { windowedArticle: Windowed<GenericRecord>, count: Long? ->
                    // project on the industry field for key
                    val windowedIndustry = Windowed(
                        windowedArticle.key()["industry"].toString(),
                        windowedArticle.window()
                    )
                    // add the page into the value
                    val viewStats: GenericRecord = GenericData.Record(schema)
                    viewStats.put("page", windowedArticle.key()["page"])
                    viewStats.put("user", "user")
                    viewStats.put("industry", windowedArticle.key()["industry"])
                    viewStats.put("count", count)
                    KeyValue(windowedIndustry, viewStats)
                },
                Grouped.with(windowedStringSerde, valueAvroSerde)
            ).aggregate( // the initializer
                { PriorityQueue(comparator) },  // the "add" aggregator
                { windowedIndustry: Windowed<String>?, record: GenericRecord, queue: PriorityQueue<GenericRecord> ->
                    queue.add(record)
                    queue
                },  // the "remove" aggregator
                { windowedIndustry: Windowed<String>?, record: GenericRecord, queue: PriorityQueue<GenericRecord> ->
                    queue.remove(record)
                    queue
                },
                Materialized.with(windowedStringSerde, PriorityQueueSerde(comparator, valueAvroSerde))
            )
        val topN = 100
        val topViewCounts = allViewCounts
            .mapValues { queue: PriorityQueue<GenericRecord> ->
                val sb = StringBuilder()
                for (i in 0 until topN) {
                    val record = queue.poll() ?: break
                    sb.append(record["page"].toString())
                    sb.append("\n")
                }
                sb.toString()
            }
        topViewCounts.toStream().to(TOP_NEWS_PER_INDUSTRY_TOPIC, Produced.with(windowedStringSerde, stringSerde))
        return KafkaStreams(builder.build(), config)
    }
}