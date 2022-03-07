import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.LogManager
import java.util.*
import kotlin.random.Random

//ExchMessagesProducer("localhost:9092").produce()
class ExchMessagesProducer(brokers: String) {
    private val logger = LogManager.getLogger(javaClass)
    private val producer = createProducer(brokers)

    private fun createProducer(brokers: String): Producer<String, Long> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokers
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.canonicalName
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = LongSerializer::class.java.canonicalName
        return KafkaProducer(props)
    }

    fun produce() {
        val now = System.currentTimeMillis()
        val delay = 1200 - Math.floorMod(now, 1000)
        val timer = Timer()
        timer.schedule(object : TimerTask() {
            override fun run() {
                val ts = System.currentTimeMillis()
                val second = Math.floorMod(ts / 1000, 60L)

                val symbol = if ((second % 2) == 0L) "VOD.L" else "BP.L"
                sendMessage(symbol, Random.Default.nextLong(100, 1000), ts)
            }
        }, delay.toLong(), 1000L)
    }

    private fun sendMessage(symbol: String, value: Long, ts: Long) {
        val futureResult = producer.send(ProducerRecord("ExchSymbols", null, ts, symbol, value))
        logger.debug("Sent a record: $symbol->$value")
        futureResult.get()
    }
}