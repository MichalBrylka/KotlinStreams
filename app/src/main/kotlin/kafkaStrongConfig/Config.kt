package kafkaStrongConfig

import java.text.NumberFormat
import java.util.*
import kotlin.reflect.*
import kotlin.reflect.full.*

/*
val props = StrongConsumerConfig().apply {
        bootstrapServers = "ABC"
        acks = Acks.All
        clientId = "123"
        enablePartitionEof = true
    }.toProperties()

    StrongConfig.fromProperties(props, ::StrongConsumerConfig)
*/

abstract class StrongConfig {
    /** Description TODO */
    @Config("acks")
    var acks: Acks? = null

    @Config("client.id")
    var clientId: String? = null

    @Config("bootstrap.servers", Importance.High)
    var bootstrapServers: String? = null

    //TODO remove
    @Config("enable.partition.eof")
    var enablePartitionEof: Boolean? = null


    companion object {
        private var numberFormat = NumberFormat.getInstance(Locale.ROOT)

        fun <T : StrongConfig> fromProperties(properties: Properties, instanceFactory: () -> T): T {
            val instance = instanceFactory()

            val meta = instance.javaClass.kotlin.memberProperties
                .map { it.findAnnotation<Config>()?.configName to it }
                .filter { it.first != null }
                .toMap()

            for ((key, value) in properties) {
                val metaProp = meta[key?.toString()]
                if (metaProp == null || value == null || metaProp !is KMutableProperty<*>) continue

                val textValue = value.toString()

                val toSet =
                    when (val propType = if (metaProp.returnType.isMarkedNullable) metaProp.returnType.withNullability(false) else metaProp.returnType) {
                        typeOf<String>() -> textValue
                        typeOf<Boolean>() -> "true".equals(textValue, true)

                        //"Enums"
                        typeOf<Acks>() -> Acks.fromNumber(textValue.toShort())


                        typeOf<Class<*>>() -> Class.forName(textValue)
                        typeOf<KClass<*>>() -> Class.forName(textValue).kotlin
                        else -> when {
                            isNumeric(propType) -> parseNumber(propType, textValue)
                            else -> value
                        }
                    }

                metaProp.setter.call(instance, toSet)
            }

            return instance
        }

        private fun isNumeric(type: KType) = when (type) {
            typeOf<Byte>() -> true
            typeOf<UByte>() -> true
            typeOf<Short>() -> true
            typeOf<UShort>() -> true
            typeOf<Int>() -> true
            typeOf<UInt>() -> true
            typeOf<Long>() -> true
            typeOf<ULong>() -> true
            typeOf<Float>() -> true
            typeOf<Double>() -> true
            else -> false
        }

        private fun parseNumber(type: KType, text: String): Any? = when (type) {
            typeOf<Byte>() -> text.toByte()
            typeOf<UByte>() -> text.toUByte()
            typeOf<Short>() -> text.toShort()
            typeOf<UShort>() -> text.toUShort()
            typeOf<Int>() -> text.toInt()
            typeOf<UInt>() -> text.toUInt()
            typeOf<Long>() -> text.toLong()
            typeOf<ULong>() -> text.toULong()
            typeOf<Float>() -> text.toFloat()
            typeOf<Double>() -> text.toDouble()
            else -> null
        }
    }

    fun toProperties(): Properties {
        val result = Properties()

        this.javaClass.kotlin.memberProperties.forEach { prop ->
            val configAnnotation = prop.findAnnotation<Config>() ?: return@forEach
            val propValue = prop.get(this) ?: return@forEach

            val text = when (propValue) {
                is String -> propValue
                is Boolean -> if (propValue) "true" else "false"
                is Number -> numberFormat.format(propValue)
                is NumericEnum<*> -> numberFormat.format(propValue.number)
                is Enum<*> -> propValue.toString()
                is Class<*> -> propValue.canonicalName
                is KClass<*> -> propValue.java.canonicalName
                else -> propValue.toString().lowercase(Locale.getDefault())
            }
            result[configAnnotation.configName] = text
        }

        return result
    }
}

class StrongConsumerConfig : StrongConfig() {
/*Consumer:
org.apache.kafka.common.serialization
* protected static Map<String, Object> appendDeserializerToConfig(Map<String, Object> configs, Deserializer<?> keyDeserializer, Deserializer<?> valueDeserializer) {
        Map<String, Object> newConfigs = new HashMap(configs);
        if (keyDeserializer != null) {
            newConfigs.put("key.deserializer", keyDeserializer.getClass());
        }

        if (valueDeserializer != null) {
            newConfigs.put("value.deserializer", valueDeserializer.getClass());
        }
* */
}
