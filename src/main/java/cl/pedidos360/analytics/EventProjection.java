package cl.pedidos360.analytics;

import java.sql.Timestamp;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class EventProjection {
    private final JdbcTemplate jdbc;
    private final EventValidator validator;

    public EventProjection(JdbcTemplate jdbc, EventValidator validator) {
        this.jdbc = jdbc;
        this.validator = validator;
    }

    @Transactional
    public void accept(String topic, String payload) {
        var e = validator.validate(topic, payload);
        if (jdbc.update("INSERT INTO event_inbox(event_id,topic) VALUES (?,?) ON CONFLICT DO NOTHING", e.id(), topic) == 0) return;
        var d = e.data();
        if (topic.equals(EventValidator.PRODUCT)) {
            jdbc.update("""
                INSERT INTO product_projection(product_id,name,category,price,stock,active,aggregate_version)
                VALUES (?,?,?,?,?,?,?) ON CONFLICT(product_id) DO UPDATE SET
                name=excluded.name,category=excluded.category,price=excluded.price,stock=excluded.stock,
                active=excluded.active,aggregate_version=excluded.aggregate_version
                WHERE product_projection.aggregate_version < excluded.aggregate_version
                """, d.path("productoId").longValue(), d.path("nombre").textValue(), d.path("categoria").textValue(),
                    d.path("precio").decimalValue(), d.path("stock").intValue(), d.path("activo").booleanValue(), e.version());
        } else {
            jdbc.update("""
                INSERT INTO interaction(event_id,tenant_id,owner_id,product_id,action,quantity,occurred_at,topic)
                VALUES (?,?,?,?,?,?,?,?)
                """, e.id(), UUID.fromString(d.path("tenantId").textValue()), UUID.fromString(d.path("ownerId").textValue()),
                    d.path("productoId").longValue(), d.path("tipo").textValue(),
                    d.has("cantidad") ? d.path("cantidad").intValue() : null, Timestamp.from(e.occurredAt()), topic);
        }
    }
}
