import kotlin.reflect.KProperty1
import kotlin.reflect.jvm.javaField

data class Person(val name: String, val age: Number, val salary: Double) : Changeable

fun main() {
    val info = "random_text,".repeat(1000)
    val tt = info.splitToSequence(",").first()
}


/**
 * Marker interface for classes that allow it's read only properties to be changed under special cases i.e. for tests
 */
interface Changeable

/**
 * Allows for visible / trackable changes of normally readonly properties
 * <b>Sample usage</b>
 *
 * ```
 * data class Person(val name: String, val age: Number, val salary: Double) : Changeable
 * data class Person2(val name: String, val age: Number, val salary: Double) : Changeable
 * data class Person3(val name: String, val age: Number, val salary: Double)
 *
 * fun main() {
 *    //Person3("Mike", 38, 15.0) withChanged (Person3::age to { 1 })//will not work with not Changeable classes
 *
 *    val p = Person("Mike", 38, 15.0)
 *    p withChanged (Person::age to { 1 })
 *    //p withChanged (Person2::age to {99}) //error - property from different type
 *    //p withChanged (Person::age to {"99"}) //error - type mismatch
 *
 *    p.withChanged(Person::age to { 2 }, Person::name to { "2" })
 *
 *    val changes = listOf(Person::age to { 3 }, Person::name to { "3" })
 *    p.withChanged(changes)
 * }
 * ```
 */
infix fun <TReceiver : Changeable, TProperty> TReceiver.withChanged(change: Pair<KProperty1<TReceiver, TProperty>, TProperty>): TReceiver = this.also {
    val (property, newValue) = change
    val field = property.javaField ?: throw UnsupportedOperationException("${property.name} does not have a corresponding Java field")

    field.isAccessible = true
    field.set(it, newValue)
}

fun <TReceiver : Changeable> TReceiver.withChanged(vararg changes: Pair<KProperty1<TReceiver, Any>, Any>): TReceiver =
    this.also { for (change in changes) it withChanged change }

fun <TReceiver : Changeable> TReceiver.withChanged(changes: Iterable<Pair<KProperty1<TReceiver, Any>, Any>>): TReceiver =
    this.also { for (change in changes) it withChanged change }


//rework to @OnlyInputTypes when this is resolved: https://youtrack.jetbrains.com/issue/KT-13198/Expose-OnlyInputTypes-annotation-to-allow-proper-checks-of-parameters-of-generic-methods-same-as-those-used-Collectioncontains
infix fun <TReceiver : Changeable, TProperty> KProperty1<TReceiver, TProperty>.to(
    that: (KProperty1<TReceiver, TProperty>) -> TProperty
) = Pair(this, that(this))