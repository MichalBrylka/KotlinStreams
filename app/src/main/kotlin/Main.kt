import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.javafaker.Faker
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.*
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import java.time.Duration
import java.time.LocalDate
import java.time.Period
import java.time.ZoneId
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.regex.Pattern
import kotlin.system.exitProcess


fun main(args: Array<String>) {
    val props = Properties()
    props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[ConsumerConfig.GROUP_ID_CONFIG] = UUID.randomUUID().toString()
    props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = LongDeserializer::class.java.canonicalName
    props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 100

    var consumer = KafkaConsumer<String, Long>(props)
    consumer.subscribe(listOf("ExchSymbols"))

    while (true) {
        val records = consumer.poll(Duration.ofSeconds(5))

        records.iterator().forEach {
            println("${it.key()}-> ${it.value()}  @ ${Math.floorMod(it.timestamp() / 1000, 60L)}    ${it.timestamp()}")
        }
    }

    consumer.close()

    //produce()
    //streams()
    //consume()
}

fun streams() {
    val props = Properties()
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "PersonTrans"
    props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"


    val streamsBuilder = StreamsBuilder()
    val personSerdes = PersonSerdes()
    val personStream = streamsBuilder.stream(
        "users",
        Consumed.with(Serdes.String(), Serdes.WrapperSerde(personSerdes, personSerdes))
    )

    val resStream = personStream.map { _, p ->
        val birthDateLocal = p.birthDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()
        val age = Period.between(birthDateLocal, LocalDate.now()).years
        KeyValue("${p.firstName} ${p.lastName}", "$age")
    }

    resStream.to("ages", Produced.with(Serdes.String(), Serdes.String()))

    val streams = KafkaStreams(streamsBuilder.build(), props)
    val latch = CountDownLatch(1)
    Runtime.getRuntime().addShutdownHook(Thread() { streams.close(); latch.countDown() })

    try {
        streams.start()
        latch.await()
    } catch (e: Throwable) {
        println("ERROR " + e.message)
        exitProcess(1)
    }
    exitProcess(0)
}

private fun consume() {
    val props = Properties()
    props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[ConsumerConfig.GROUP_ID_CONFIG] = "xxx"
    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = PersonSerdes::class.java.canonicalName
    props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 100

    var consumer = KafkaConsumer<String, Person>(props)
    consumer.subscribe(listOf("users"))

    while (true) {
        val records = consumer.poll(Duration.ofSeconds(5))

        records.iterator().forEach {
            val person = it.value()
            val birthDateLocal = person.birthDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()
            val age = Period.between(birthDateLocal, LocalDate.now()).years

            println("${person.fullName} @$age")
        }

    }

    consumer.close()
}

private fun produce() {
    val props = Properties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.canonicalName
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = PersonSerdes::class.java.canonicalName
    var producer = KafkaProducer<String, Person>(props)


    val faker = Faker()

    for (i in 1..100) {
        val fakePerson = Person(
            firstName = faker.name().firstName(),
            lastName = faker.name().lastName(),
            birthDate = faker.date().birthday()
        )
        val record = ProducerRecord("users", fakePerson.fullName, fakePerson)
        producer.send(record, DemoProducerCallback())
    }
    producer.close()
}

private class DemoProducerCallback : Callback {
    override fun onCompletion(recordMetadata: RecordMetadata, e: Exception?) {
        if (e != null) {
            println("Error producing to topic " + recordMetadata.topic())
            e.printStackTrace()
        }

    }
}

class PersonSerdes : JsonSerDes<Person>(Person::class.java)

open class JsonSerDes<T>(private val tClass: Class<T>) : Serializer<T>, Deserializer<T> {
    private val jsonMapper = ObjectMapper().apply {
        registerKotlinModule()
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        dateFormat = StdDateFormat()
    }

    override fun serialize(topic: String?, data: T?): ByteArray? =
        if (data == null) null else jsonMapper.writeValueAsBytes(data)

    override fun deserialize(topic: String, data: ByteArray?): T? =
        if (data == null) null else jsonMapper.readValue(data, tClass)

    override fun close() {}
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
}

data class Person(val firstName: String, val lastName: String, val birthDate: Date) {
    @JsonIgnore
    val fullName = "$firstName $lastName"
}