package cl.pedidos360.analytics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.core.JsonParser;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class EventValidator {
    public static final String PRODUCT = "producto.actualizado.v1";
    public static final String INTERACTION = "producto.interaccion.v1";
    public static final String CART = "carrito.actualizado.v1";
    private final ObjectMapper mapper;

    public EventValidator(ObjectMapper mapper) { this.mapper = mapper; }

    public record Event(UUID id, String topic, String aggregateId, long version, Instant occurredAt, JsonNode data) {}

    public static class InvalidEvent extends RuntimeException {
        public InvalidEvent() { super("INVALID_EVENT", null, false, false); }
    }

    public Event validate(String topic, String payload) {
        try {
            require(payload != null && payload.length() <= 65536);
            JsonNode e = mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .with(JsonParser.Feature.STRICT_DUPLICATE_DETECTION).readTree(payload);
            require(e != null && e.isObject());
            uuid(text(e, "eventId"));
            uuid(text(e, "correlationId"));
            require(Set.of(PRODUCT, INTERACTION, CART).contains(topic));
            require(topic.equals(text(e, "eventType")));
            require(e.path("schemaVersion").isIntegralNumber() && e.path("schemaVersion").canConvertToInt()
                    && e.path("schemaVersion").intValue() == 1);
            require((topic.equals(CART) ? "ms-carrito" : "ms-productos").equals(text(e, "producer")));
            String aggregate = text(e, "aggregateId");
            require(aggregate.length() <= 200);
            require(e.path("aggregateVersion").isIntegralNumber() && e.path("aggregateVersion").canConvertToLong());
            long version = e.path("aggregateVersion").longValue();
            require(version > 0);
            Instant occurred = Instant.parse(text(e, "occurredAt"));
            require(!occurred.isAfter(Instant.now().plusSeconds(300)));
            JsonNode d = e.path("data");
            require(d.isObject());
            positiveLong(d, "productoId");
            if (topic.equals(PRODUCT)) {
                require(aggregate.equals(Long.toString(d.path("productoId").longValue())));
                require(text(d, "nombre").length() <= 250);
                require(text(d, "categoria").length() <= 150);
                require(d.path("precio").isNumber() && d.path("precio").decimalValue().signum() >= 0
                        && d.path("precio").decimalValue().scale() <= 4
                        && d.path("precio").decimalValue().compareTo(new BigDecimal("1000000000000000")) < 0);
                require(d.path("stock").isIntegralNumber() && d.path("stock").canConvertToInt() && d.path("stock").intValue() >= 0);
                require(d.path("activo").isBoolean());
            } else {
                uuid(text(d, "tenantId"));
                uuid(text(d, "ownerId"));
                String action = text(d, "tipo");
                require((topic.equals(CART) ? Set.of("ADD", "UPDATE", "REMOVE") : Set.of("VIEW", "SELECT", "ADD", "UPDATE", "REMOVE")).contains(action));
                if (d.has("cantidad")) {
                    require(d.path("cantidad").isIntegralNumber() && d.path("cantidad").canConvertToInt());
                    int quantity = d.path("cantidad").intValue();
                    require("REMOVE".equals(action) ? quantity == 0 : quantity > 0);
                }
            }
            return new Event(uuid(text(e, "eventId")), topic, aggregate, version, occurred, d);
        } catch (Exception ignored) {
            // Never retain the original exception: JSON parser errors may contain personal payloads.
            throw new InvalidEvent();
        }
    }

    private static void positiveLong(JsonNode n, String key) {
        require(n.path(key).isIntegralNumber() && n.path(key).canConvertToLong() && n.path(key).longValue() > 0);
    }
    private static String text(JsonNode n, String key) {
        require(n.path(key).isTextual() && !n.path(key).textValue().isBlank());
        return n.path(key).textValue();
    }
    private static UUID uuid(String value) {
        UUID uuid = UUID.fromString(value);
        require(uuid.toString().equalsIgnoreCase(value));
        return uuid;
    }
    private static void require(boolean valid) { if (!valid) throw new InvalidEvent(); }
}
