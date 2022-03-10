import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory


class DynamicTests {
    @TestFactory
    fun multiply_ShouldMultiplyNumbers(): List<DynamicTest> =
        arrayOf(listOf(1, 2, 2), listOf(5, 3, 15), listOf(121, 4, 484)).map {
            val (a, b, expected) = it
            dynamicTest("$a * $b == $expected") { assertThat(multiply(a, b)).isEqualTo(expected) }
        }
}

fun multiply(i: Int, j: Int): Int = i * j

