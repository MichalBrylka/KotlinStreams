import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class DynamicTestCreationTest {

@TestFactory
Stream<DynamicTest> Multiply_ShouldMultiplyNumbers() {
    int[][] data = new int[][] { { 1, 2, 2 }, { 5, 3, 15 }, { 121, 4, 484 } };
    return Arrays.stream(data).map(entry -> {
        int m1 = entry[0];
        int m2 = entry[1];
        int expected = entry[2];
        return dynamicTest(MessageFormat.format("{0} * {1} = {2}", m1, m2, expected),
                () -> assertEquals(expected, Calc.multiply(m1, m2)));
    });
}

// class to be tested
static class Calc {
    public static int multiply(int i, int j) {
        return i * j;
    }
}
}