package cl.pedidos360.analytics;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

@Component
@ConditionalOnProperty(name = "analytics.dynamodb.enabled", havingValue = "true")
class DynamoHistory {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DynamoHistory.class);
    private final DynamoDbClient dynamo;
    private final JdbcTemplate db;
    private final TransactionTemplate tx;
    private final String table;
    private final Duration retention;

    DynamoHistory(DynamoDbClient dynamo, JdbcTemplate db, TransactionTemplate tx,
                  @Value("${analytics.dynamodb.table}") String table,
                  @Value("${analytics.dynamodb.retention-days}") int retentionDays) {
        if (!table.matches("[A-Za-z0-9_.-]{3,255}") || retentionDays < 1 || retentionDays > 365)
            throw new IllegalArgumentException("INVALID_ANALYTICS_ARCHIVE_CONFIGURATION");
        this.dynamo = dynamo; this.db = db; this.tx = tx; this.table = table;
        this.retention = Duration.ofDays(retentionDays);
    }

    @Scheduled(fixedDelay = 2000)
    void writePending() {
        for (int i = 0; i < 12; i++) {
            if (!Boolean.TRUE.equals(tx.execute(status -> writeOne()))) return;
        }
    }

    private boolean writeOne() {
        var events = db.query("""
                SELECT i.event_id,i.tenant_id,i.owner_id,i.product_id,i.action,i.occurred_at,i.topic
                FROM analytics_archive_pending a JOIN interaction i ON i.event_id=a.event_id
                WHERE a.next_attempt_at <= clock_timestamp() ORDER BY a.next_attempt_at,a.event_id
                LIMIT 1 FOR UPDATE OF a SKIP LOCKED
                """, (rs, row) -> new ArchivedEvent(rs.getObject("event_id", UUID.class),
                rs.getObject("tenant_id", UUID.class), rs.getObject("owner_id", UUID.class),
                rs.getLong("product_id"), rs.getString("action"), rs.getString("topic"),
                rs.getObject("occurred_at", OffsetDateTime.class)));
        if (events.isEmpty()) return false;
        var event = events.getFirst();
        try {
            long expiresAt = event.at().toInstant().plus(retention).getEpochSecond();
            if (expiresAt > java.time.Instant.now().getEpochSecond()) {
                dynamo.putItem(PutItemRequest.builder().tableName(table).item(event.item(expiresAt))
                        .conditionExpression("attribute_not_exists(pk)").build());
            }
        } catch (ConditionalCheckFailedException alreadyArchived) {
            // A prior attempt reached DynamoDB before committing the PostgreSQL acknowledgement.
        } catch (SdkException unavailable) {
            db.update("UPDATE analytics_archive_pending SET next_attempt_at=now()+interval '30 seconds' WHERE event_id=?", event.id());
            LOG.warn("DynamoDB archive unavailable; delivery remains pending");
            return false;
        }
        db.update("DELETE FROM analytics_archive_pending WHERE event_id=?", event.id());
        return true;
    }

    record ArchivedEvent(UUID id, UUID tenant, UUID owner, long product, String action, String topic, OffsetDateTime at) {
        Map<String, AttributeValue> item(long expiresAt) {
            return Map.of("pk", string(tenant + "#" + owner), "sk", string(id.toString()),
                    "productId", AttributeValue.fromN(Long.toString(product)), "action", string(action),
                    "topic", string(topic), "occurredAt", string(at.toInstant().toString()),
                    "expiresAt", AttributeValue.fromN(Long.toString(expiresAt)));
        }

        private static AttributeValue string(String value) { return AttributeValue.fromS(value); }
    }

    @Configuration
    @EnableScheduling
    @ConditionalOnProperty(name = "analytics.dynamodb.enabled", havingValue = "true")
    static class ClientConfig {
        @Bean(destroyMethod = "close")
        DynamoDbClient analyticsDynamo(@Value("${analytics.dynamodb.region}") String region) {
            return DynamoDbClient.builder().region(Region.of(region)).overrideConfiguration(config ->
                    config.apiCallAttemptTimeout(Duration.ofSeconds(2)).apiCallTimeout(Duration.ofSeconds(4))).build();
        }
    }
}
