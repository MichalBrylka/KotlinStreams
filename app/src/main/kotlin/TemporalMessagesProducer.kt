import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.LogManager
import java.util.*

//TemporalMessagesProducer("localhost:9092").produce()
class TemporalMessagesProducer(brokers: String) {
    private val logger = LogManager.getLogger(javaClass)
    private val producer = createProducer(brokers)

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokers
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.canonicalName
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.canonicalName
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

                if (second != 58L) {
                    sendMessage(second, ts, "on time")
                }
                if (second == 2L) {
                    // send the late record
                    sendMessage(58, ts - 4000, "late")
                }
            }
        }, delay.toLong(), 1000L)
    }

    private fun sendMessage(id: Long, ts: Long, info: String) {
        val window = (ts / 10000) * 10000
        val value = "$window,$id,$info"
        val futureResult = producer.send(ProducerRecord("events", null, ts, "$id", value))
        logger.debug("Sent a record: $value")
        futureResult.get()
    }
}