package kafkaStrongConfig

enum class Importance { Low, Medium, High }

@Target(AnnotationTarget.PROPERTY)
annotation class Config(val configName: String, val importance: Importance = Importance.Low)