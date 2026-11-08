
package util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class LiftVerticalLookup {

    private static final Map<Integer, Integer> LIFT_VERTICALS = load();

    private LiftVerticalLookup() {
        // Utility class; prevent instantiation.
    }

    private static Map<Integer, Integer> load() {
        try (InputStream stream = LiftVerticalLookup.class
                .getClassLoader()
                .getResourceAsStream("lift_vertical.json")) {

            if (stream == null) {
                throw new IllegalStateException(
                        "lift_vertical.json not found in classpath");
            }

            JsonObject root = new Gson().fromJson(
                    new InputStreamReader(
                            stream, StandardCharsets.UTF_8),
                    JsonObject.class
            );

            if (root == null ||
                    !root.has("lifts") ||
                    !root.get("lifts").isJsonObject()) {
                throw new IllegalStateException(
                        "Invalid lift_vertical.json structure");
            }

            JsonObject lifts = root.getAsJsonObject("lifts");
            Map<Integer, Integer> values = new HashMap<>();

            for (int liftId = 1; liftId <= 40; liftId++) {
                String key = String.valueOf(liftId);

                if (!lifts.has(key)) {
                    throw new IllegalStateException(
                            "Missing lift ID: " + liftId);
                }

                int vertical = lifts.get(key).getAsInt();

                if (vertical < 60 || vertical > 400) {
                    throw new IllegalStateException(
                            "Invalid vertical for lift " + liftId);
                }

                values.put(liftId, vertical);
            }

            if (lifts.size() != 40) {
                throw new IllegalStateException(
                        "Expected exactly 40 lift mappings");
            }

            return Collections.unmodifiableMap(values);

        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to load lift vertical lookup", e);
        }
    }

    public static int getVertical(int liftId) {
        Integer vertical = LIFT_VERTICALS.get(liftId);

        if (vertical == null) {
            throw new IllegalArgumentException(
                    "Unknown lift ID: " + liftId);
        }

        return vertical;
    }

    public static void main(String[] args) {
        System.out.println("Lift 1: " + getVertical(1));
        System.out.println("Lift 17: " + getVertical(17));
        System.out.println("Lift 40: " + getVertical(40));
        System.out.println("Total lift mappings: " +
                LIFT_VERTICALS.size());
    }
}
