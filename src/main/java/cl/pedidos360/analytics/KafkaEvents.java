package cl.pedidos360.analytics;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaEvents {
    private final EventProjection projection;

    public KafkaEvents(EventProjection projection) { this.projection = projection; }

    @KafkaListener(id = "analytics-events", idIsGroup = false,
            topics = {EventValidator.PRODUCT, EventValidator.INTERACTION, EventValidator.CART})
    public void consume(ConsumerRecord<String, String> record) {
        // The proxied method returns only after PostgreSQL commit. RECORD ack follows return.
        projection.accept(record.topic(), record.value());
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka) {
        var handler = new DefaultErrorHandler((record, exception) -> {
            String reason = hasInvalidCause(exception) ? "INVALID_EVENT" : "PROCESSING_FAILED";
            jdbc.update("""
                INSERT INTO event_quarantine(topic,partition_id,record_offset,reason) VALUES (?,?,?,?)
                ON CONFLICT DO NOTHING
                """, record.topic(), record.partition(), record.offset(), reason);
            // No raw body, key, headers, identity, or exception details leave the consumer.
            String safe = "{\"topic\":\"" + record.topic() + "\",\"partition\":" + record.partition()
                    + ",\"offset\":" + record.offset() + ",\"reason\":\"" + reason + "\"}";
            try {
                kafka.send(new ProducerRecord<>(record.topic() + ".DLT", null, safe)).get(15, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                throw new IllegalStateException("QUARANTINE_DELIVERY_FAILED");
            }
        }, new FixedBackOff(1000L, 2L));
        handler.addNotRetryableExceptions(EventValidator.InvalidEvent.class);
        handler.setAckAfterHandle(true);
        return handler;
    }

    private static boolean hasInvalidCause(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) if (t instanceof EventValidator.InvalidEvent) return true;
        return false;
    }
}
