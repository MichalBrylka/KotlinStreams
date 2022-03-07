import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.*
import java.time.Duration
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess


//ExchMessagesProcessor("localhost:9092").process()
class ExchMessagesProcessor(private val brokers: String) {
    fun process() {
        val streamsBuilder = StreamsBuilder()

        val eventStream = streamsBuilder
            .stream("ExchSymbols", Consumed.with(Serdes.String(), Serdes.Long()))

        /*val windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(String::class.java, windowSize.toMillis())



        val aggregates =
            eventStream
                    //TODO +filter
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .windowedBy(TimeWindows.of(Duration.ofSeconds(60)).advanceBy(Duration.ofSeconds(10)))
                .aggregate()
            .count(Materialized.with(Serdes.String(), Serdes.Long()))

        aggregates.toStream().to("ExchWindows")*/

        /*aggregates
            .toStream()
            .map { ws, i -> KeyValue("${ws.window().start()}", "$i") }
            .to("ExchWindows", Produced.with(Serdes.String(), Serdes.String()))*/

        val topology = streamsBuilder.build()

        start(topology)
    }

    private fun start(topology: Topology) {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-tutorial-events-4"
        props["auto.offset.reset"] = "latest"
        props["commit.interval.ms"] = 0
        props[StreamsConfig.STATE_DIR_CONFIG] = "c:\\temp";
        val streams = KafkaStreams(topology, props)

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
}