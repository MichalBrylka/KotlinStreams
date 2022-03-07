import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import java.io.*
import java.nio.BufferUnderflowException
import java.util.*

public class PriorityQueueSerde<T>(comparator: Comparator<T>, avroSerde: Serde<T>) : Serde<PriorityQueue<T>> {
    private val inner: Serde<PriorityQueue<T>?> = Serdes.serdeFrom(
        PriorityQueueSerializer<T>(avroSerde.serializer()),
        PriorityQueueDeserializer(comparator, avroSerde.deserializer())
    )

    override fun serializer(): Serializer<PriorityQueue<T>?> = inner.serializer()

    override fun deserializer(): Deserializer<PriorityQueue<T>?> = inner.deserializer()

    override fun close() {
        inner.serializer().close()
        inner.deserializer().close()
    }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        inner.serializer().configure(configs, isKey)
        inner.deserializer().configure(configs, isKey)
    }
}

class PriorityQueueSerializer<T>(private val valueSerializer: Serializer<T>) : Serializer<PriorityQueue<T>?> {
    override fun serialize(topic: String?, queue: PriorityQueue<T>?): ByteArray {
        val outStream = ByteArrayOutputStream()
        DataOutputStream(outStream).use {
            try {
                it.writeInt(queue?.size ?: 0) //size

                if (queue != null)
                    for (element in queue) {
                        val bytes: ByteArray = valueSerializer.serialize(topic, element)
                        it.writeInt(bytes.size)
                        it.write(bytes)
                    }
                it.flush()
                return outStream.toByteArray()
            } catch (e: IOException) {
                throw RuntimeException("unable to serialize PriorityQueue", e)
            }
        }
    }
}

class PriorityQueueDeserializer<T>(private val comparator: Comparator<T>, private val valueDeserializer: Deserializer<T>) : Deserializer<PriorityQueue<T>?> {
    override fun deserialize(s: String?, bytes: ByteArray?): PriorityQueue<T>? {
        if (bytes == null) return null
        if (bytes.isEmpty()) return PriorityQueue(comparator)

        DataInputStream(ByteArrayInputStream(bytes)).use {
            try {
                val priorityQueue = PriorityQueue(comparator)

                val records: Int = it.readInt()
                for (i in 0 until records) {
                    val elementSize = it.readInt()
                    val valueBytes = ByteArray(elementSize)
                    if (it.read(valueBytes) != elementSize) throw BufferUnderflowException()

                    priorityQueue.add(valueDeserializer.deserialize(s, valueBytes))
                }

                return priorityQueue
            } catch (e: IOException) {
                throw RuntimeException("Unable to deserialize PriorityQueue", e)
            }
        }
    }
}