import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.kstream.WindowedSerdes
import java.time.Duration
import java.util.*
import java.util.stream.IntStream

@Suppress("unused")
object TopArticlesExampleDriver {
    fun main(args: Array<String?>) {
        val bootstrapServers = if (args.isNotEmpty()) args[0] else "localhost:9092"
        val schemaRegistryUrl = if (args.size > 1) args[1] else "http://localhost:8081"
        //produceInputs(bootstrapServers, schemaRegistryUrl);
        consumeOutput(bootstrapServers, schemaRegistryUrl)
        //streams(bootstrapServers, schemaRegistryUrl);
    }

    fun streams(bootstrapServers: String, schemaRegistryUrl: String) {
        val streams: KafkaStreams = TopArticlesLambdaExample.buildTopArticlesStream(bootstrapServers, schemaRegistryUrl, "/tmp/kafka-streams")
        streams.cleanUp()
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    }

    private fun produceInputs(bootstrapServers: String, schemaRegistryUrl: String) {
        val users = arrayOf("erica", "bob", "joe", "damian", "tania", "phil", "sam", "lauren", "joseph")
        val industries = arrayOf("engineering", "telco", "finance", "health", "science")
        val pages = arrayOf("index.html", "news.html", "contact.html", "about.html", "stuff.html")
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = io.confluent.kafka.serializers.KafkaAvroSerializer::class.java
        props[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl
        val producer: KafkaProducer<String, GenericRecord> = KafkaProducer(props)

        val schema = """
                {"namespace": "io.confluent.examples.streams.avro",
                 "type": "record",
                 "name": "PageView",
                 "fields": [
                     {"name": "user", "type": "string"},
                     {"name": "page", "type": "string"},
                     {"name": "industry", "type": "string"},
                     {"name": "flags", "type": ["null", "string"], "default": null}
                 ]
                }""".trimIndent()


        val pageViewBuilder = GenericRecordBuilder(loadSchema(schema))
        val random = Random()
        for (user in users) {
            pageViewBuilder.set("industry", industries[random.nextInt(industries.size)])
            pageViewBuilder.set("flags", "ARTICLE")
            // For each user generate some page views
            IntStream.range(0, random.nextInt(50, 100))
                .mapToObj {
                    pageViewBuilder.set("user", user)
                    pageViewBuilder.set("page", pages[random.nextInt(pages.size)])
                    pageViewBuilder.build()
                }.forEach { record -> producer.send(ProducerRecord(TopArticlesLambdaExample.PAGE_VIEWS, null, record)) }
        }
        producer.flush()
    }

    private fun consumeOutput(bootstrapServers: String?, schemaRegistryUrl: String?) {
        val props = Properties()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl
        props[ConsumerConfig.GROUP_ID_CONFIG] = "top-articles-lambda-example-consumer"
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

        val windowedDeserializer: Deserializer<Windowed<String>> =
            WindowedSerdes.timeWindowedSerdeFrom(String::class.java, TopArticlesLambdaExample.windowSize.toMillis()).deserializer()
        val consumer: KafkaConsumer<Windowed<String>, String> = KafkaConsumer(props, windowedDeserializer, Serdes.String().deserializer())
        consumer.subscribe(listOf(TopArticlesLambdaExample.TOP_NEWS_PER_INDUSTRY_TOPIC))
        while (true) {
            val consumerRecords = consumer.poll(Duration.ofMillis(Long.MAX_VALUE))
            for (consumerRecord in consumerRecords)
                println(consumerRecord.key().key() + "@" + consumerRecord.key().window().start() + "=" + consumerRecord.value())
        }
    }

    private fun loadSchema(schema: String?): Schema = Schema.Parser().parse(schema)
    /*fun loadSchemaFromResource(name: String): Schema {
        TopArticlesLambdaExample::class.java
            .classLoader
            .getResourceAsStream("avro/io/confluent/examples/streams/$name").use { input -> return Schema.Parser().parse(input) }
    }*/
}