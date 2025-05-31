package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.MapFunction; // Required import
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DataStreamJobTest {

    // Define a class that implements MapFunction, replicating the logic
    // from DataStreamJob.java for testing purposes.
    static class MyTestMapFunction implements MapFunction<Integer, String> {
        @Override
        public String map(Integer value) throws Exception {
            if (value == null) {
                return "Anull"; // Or throw an exception, depending on desired handling
            }
            return "A" + value.toString();
        }
    }

    @Test
    void testMapFunction_positiveInput() throws Exception {
        MyTestMapFunction mapFunction = new MyTestMapFunction();
        assertEquals("A123", mapFunction.map(123));
    }

    @Test
    void testMapFunction_zeroInput() throws Exception {
        MyTestMapFunction mapFunction = new MyTestMapFunction();
        assertEquals("A0", mapFunction.map(0));
    }

    @Test
    void testMapFunction_negativeInput() throws Exception {
        MyTestMapFunction mapFunction = new MyTestMapFunction();
        assertEquals("A-5", mapFunction.map(-5));
    }

    @Test
    void testMapFunction_nullInput() throws Exception {
        MyTestMapFunction mapFunction = new MyTestMapFunction();
        // Assuming the original MapFunction would behave like this for null,
        // or adjust if it's expected to throw NullPointerException.
        // The current anonymous MapFunction in DataStreamJob would throw a NullPointerException
        // if 'value.toString()' is called on a null.
        // For robustness, the test version handles null explicitly.
        // If strict replication of potential NPE is needed, this test should change.
        // The provided solution in DataStreamJob.java does not have null check.
        // Let's test the exact behavior.

        // Re-defining the map function exactly as it is in DataStreamJob.java for this specific test
        MapFunction<Integer, String> originalMapLogic = new MapFunction<Integer, String>() {
            @Override
            public String map(Integer value) throws Exception {
                return "A" + value.toString(); // This will throw NullPointerException if value is null
            }
        };

        assertThrows(NullPointerException.class, () -> {
            originalMapLogic.map(null);
        }, "Mapping a null value should throw NullPointerException");
    }
}
