package cl.pedidos360.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.*;

class EventValidatorTest {
    static final ObjectMapper JSON = new ObjectMapper();
    final EventValidator validator = new EventValidator(JSON);

    static ObjectNode event() {
        var e = JSON.createObjectNode();
        e.put("eventId", UUID.randomUUID().toString()).put("eventType", EventValidator.PRODUCT).put("schemaVersion", 1)
                .put("occurredAt", Instant.now().toString()).put("producer", "ms-productos").put("aggregateId", "1")
                .put("aggregateVersion", 1).put("correlationId", UUID.randomUUID().toString());
        e.putObject("data").put("productoId", 1).put("nombre", "Pizza").put("categoria", "food")
                .put("precio", 12.5).put("stock", 2).put("activo", true);
        return e;
    }

    static Stream<Consumer<ObjectNode>> mutations() {
        return Stream.of(
                e -> e.remove("eventId"), e -> e.put("eventId", "1-1-1-1-1"),
                e -> e.put("correlationId", "not-uuid"), e -> e.put("eventType", "unknown"),
                e -> e.put("schemaVersion", 2), e -> e.put("schemaVersion", 4294967297L), e -> e.put("schemaVersion", "1"),
                e -> e.put("producer", "ms-carrito"), e -> e.put("aggregateId", "2"),
                e -> e.put("aggregateVersion", 0), e -> e.put("aggregateVersion", 1.5),
                e -> e.put("aggregateVersion", "1"), e -> e.put("occurredAt", "yesterday"),
                e -> e.put("occurredAt", Instant.now().plusSeconds(3600).toString()),
                e -> ((ObjectNode) e.path("data")).put("productoId", -1),
                e -> ((ObjectNode) e.path("data")).put("productoId", "1"),
                e -> ((ObjectNode) e.path("data")).put("nombre", " "),
                e -> ((ObjectNode) e.path("data")).put("nombre", "x".repeat(251)),
                e -> ((ObjectNode) e.path("data")).put("categoria", "x".repeat(151)),
                e -> ((ObjectNode) e.path("data")).put("precio", -1),
                e -> ((ObjectNode) e.path("data")).put("precio", 1000000000000000L),
                e -> ((ObjectNode) e.path("data")).put("precio", 1.00001),
                e -> ((ObjectNode) e.path("data")).put("stock", 2147483648L),
                e -> ((ObjectNode) e.path("data")).put("stock", -1),
                e -> ((ObjectNode) e.path("data")).put("activo", "true"));
    }

    @ParameterizedTest @MethodSource("mutations")
    void rejectsInvalidContractWithoutRetainingPayload(Consumer<ObjectNode> mutation) {
        var event = event();
        mutation.accept(event);
        assertThatThrownBy(() -> validator.validate(EventValidator.PRODUCT, event.toString()))
                .isInstanceOf(EventValidator.InvalidEvent.class).hasMessage("INVALID_EVENT").hasNoCause();
    }

    @Test void strictJsonAndBodySize() {
        for (String body : new String[]{"{private:secret}", "[]", "null", event() + " {}", "x".repeat(65537),
                event().toString().replace("\"schemaVersion\":1", "\"schemaVersion\":1,\"schemaVersion\":1")}) {
            assertThatThrownBy(() -> validator.validate(EventValidator.PRODUCT, body)).isInstanceOf(EventValidator.InvalidEvent.class).hasNoCause();
        }
        assertThatThrownBy(() -> validator.validate(EventValidator.PRODUCT, null)).isInstanceOf(EventValidator.InvalidEvent.class);
    }

    @Test void acceptsCanonicalProductsAndValidatesInteractionIdentityAndAction() {
        assertThat(validator.validate(EventValidator.PRODUCT, event().toString()).version()).isEqualTo(1);
        var event = event();
        event.put("eventType", EventValidator.CART).put("producer", "ms-carrito");
        event.set("data", JSON.valueToTree(Map.of("tenantId", UUID.randomUUID().toString(), "ownerId", UUID.randomUUID().toString(),
                "productoId", 1, "tipo", "ADD", "cantidad", 2)));
        assertThat(validator.validate(EventValidator.CART, event.toString()).data().path("tipo").textValue()).isEqualTo("ADD");
        var data = (ObjectNode) event.path("data");
        data.put("tipo", "VIEW");
        assertThatThrownBy(() -> validator.validate(EventValidator.CART, event.toString())).isInstanceOf(EventValidator.InvalidEvent.class);
        event.put("eventType", EventValidator.INTERACTION).put("producer", "ms-productos");
        assertThat(validator.validate(EventValidator.INTERACTION, event.toString())).isNotNull();
        data.put("tipo", "INVALID");
        assertThatThrownBy(() -> validator.validate(EventValidator.INTERACTION, event.toString())).isInstanceOf(EventValidator.InvalidEvent.class);
        data.put("tipo", "VIEW").put("cantidad", 0);
        assertThatThrownBy(() -> validator.validate(EventValidator.INTERACTION, event.toString())).isInstanceOf(EventValidator.InvalidEvent.class);
        data.remove("cantidad");
        data.put("ownerId", "user@example.test");
        assertThatThrownBy(() -> validator.validate(EventValidator.INTERACTION, event.toString())).isInstanceOf(EventValidator.InvalidEvent.class);
        data.put("ownerId", UUID.randomUUID().toString()).put("tenantId", "bad");
        assertThatThrownBy(() -> validator.validate(EventValidator.INTERACTION, event.toString())).isInstanceOf(EventValidator.InvalidEvent.class);
    }

    @Test void removedCartItemHasZeroRemainingQuantity() {
        var e = event();
        e.put("eventType", EventValidator.CART).put("producer", "ms-carrito");
        e.set("data", JSON.valueToTree(Map.of("tenantId", UUID.randomUUID().toString(), "ownerId", UUID.randomUUID().toString(),
                "productoId", 1, "tipo", "REMOVE", "cantidad", 0)));
        assertThat(validator.validate(EventValidator.CART, e.toString()).data().path("cantidad").asInt()).isZero();
        ((ObjectNode) e.path("data")).put("cantidad", -1);
        assertThatThrownBy(() -> validator.validate(EventValidator.CART, e.toString())).isInstanceOf(EventValidator.InvalidEvent.class);
    }
}
