package kafkaStrongConfig

abstract class NumericEnum<TNumber : Number>(val number: TNumber)

interface NumericEnumFactory<TNumber : Number, TEnum> {
     fun fromNumber(number: TNumber): TEnum
}

class Acks(value: Short) : NumericEnum<Short>(value) {
    companion object : NumericEnumFactory<Short, Acks> {
        override fun fromNumber(number: Short): Acks = Acks(number)

        val None: Acks = Acks(0)
        val Leader: Acks = Acks(1)
        val All: Acks = Acks(-1)
    }
}