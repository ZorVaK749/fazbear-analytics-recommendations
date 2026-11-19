package cl.pedidos360.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.*;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.socket.*;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@ActiveProfiles("local")
@Import(AnalyticsIntegrationTest.Topics.class)
class AnalyticsIntegrationTest {
    static final UUID TENANT = UUID.fromString("ee0d2c5b-e60b-4c57-9b30-8a38a520c430");
    static final String AUDIENCE = "304fa289-2b72-447b-9c48-f00998f7c22f";
    static final UUID A = UUID.randomUUID(), B = UUID.randomUUID(), OTHER_TENANT = UUID.randomUUID();
    static final ObjectMapper JSON = new ObjectMapper();
    static final RSAKey KEY;
    static final HttpServer JWKS;
    static {
        try {
            KEY = new RSAKeyGenerator(2048).keyID("local-test").generate();
            JWKS = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            JWKS.createContext("/jwks", exchange -> {
                byte[] bytes = new JWKSet(KEY.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (var output = exchange.getResponseBody()) { output.write(bytes); }
            });
            JWKS.start();
        } catch (Exception e) { throw new ExceptionInInitializerError(e); }
    }
    @Container static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
    @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("LOCAL_JWT_JWK_SET_URI", () -> "http://127.0.0.1:" + JWKS.getAddress().getPort() + "/jwks");
        registry.add("analytics.dashboard.enabled", () -> "true");
        registry.add("analytics.dashboard.url", () -> "wss://analytics.example.test/ws/analytics");
        registry.add("analytics.dashboard.origin", () -> "https://frontend.example.test");
        registry.add("analytics.dynamodb.enabled", () -> "true");
        registry.add("analytics.dynamodb.table", () -> "test-history");
    }

    @TestConfiguration
    static class Topics {
        @Bean KafkaAdmin.NewTopics topics() {
            return new KafkaAdmin.NewTopics(new NewTopic(EventValidator.PRODUCT, 3, (short) 1),
                    new NewTopic(EventValidator.INTERACTION, 3, (short) 1), new NewTopic(EventValidator.CART, 3, (short) 1),
                    new NewTopic(EventValidator.PRODUCT + ".DLT", 3, (short) 1),
                    new NewTopic(EventValidator.INTERACTION + ".DLT", 3, (short) 1),
                    new NewTopic(EventValidator.CART + ".DLT", 3, (short) 1));
        }
    }

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired JdbcTemplate jdbc;
    @Autowired EventProjection projection;
    @Autowired RecommendationService recommendations;
    @Autowired KafkaListenerEndpointRegistry listeners;
    @Autowired MockMvc mvc;
    @MockitoBean DynamoDbClient dynamo;
    @LocalServerPort int httpPort;

    @BeforeEach void clean() {
        jdbc.execute("TRUNCATE analytics_archive_pending,dashboard_ticket,interaction,event_inbox,product_projection,event_quarantine");
    }

    @AfterAll static void stopJwks() { JWKS.stop(0); }

    @Test
    void kafkaEventsDriveOwnerScopedMetricsOverNativeWebSocketAndTicketIsSingleUse() throws Exception {
        product(91, "Pizza", "food", 8, true, 1);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(count("product_projection")).isEqualTo(1));
        interaction(TENANT, A, 91, "VIEW", EventValidator.INTERACTION);
        interaction(TENANT, B, 91, "VIEW", EventValidator.INTERACTION);
        awaitCount(2);
        mvc.perform(post("/api/analytics/me/dashboard-ticket")).andExpect(status().isUnauthorized());
        String issued = mvc.perform(post("/api/analytics/me/dashboard-ticket")
                        .header("Authorization", "Bearer " + token(TENANT, A, true)))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        String ticket = JSON.readTree(issued).get("ticket").asText();
        var events = new java.util.concurrent.LinkedBlockingQueue<String>();
        var headers = new WebSocketHttpHeaders();
        headers.setOrigin("https://frontend.example.test");
        var url = java.net.URI.create("ws://127.0.0.1:" + httpPort + "/ws/analytics");
        var client = new StandardWebSocketClient();
        try (var socket = client.execute(new TextWebSocketHandler() {
            @Override protected void handleTextMessage(WebSocketSession connection, TextMessage message) {
                events.offer(message.getPayload());
            }
        }, headers, url).get(10, TimeUnit.SECONDS)) {
            socket.sendMessage(new TextMessage(ticket));
            var first = JSON.readTree(events.poll(10, TimeUnit.SECONDS));
            assertThat(first.path("interacciones").asLong()).isEqualTo(1);
            assertThat(first.at("/productosMasVistos/0/visitas").asLong()).isEqualTo(1);
            assertThat(first.at("/categorias/0/categoria").asText()).isEqualTo("food");
            interaction(TENANT, A, 91, "ADD", EventValidator.CART);
            var update = JSON.readTree(events.poll(10, TimeUnit.SECONDS));
            assertThat(update.path("interacciones").asLong()).isEqualTo(2);
            assertThat(update.at("/categorias/0/interacciones").asLong()).isEqualTo(2);
        }
        var closed = new java.util.concurrent.CompletableFuture<CloseStatus>();
        try (var replay = client.execute(new TextWebSocketHandler() {
            @Override public void afterConnectionClosed(WebSocketSession connection, CloseStatus reason) {
                closed.complete(reason);
            }
        }, headers, url).get(10, TimeUnit.SECONDS)) {
            replay.sendMessage(new TextMessage(ticket));
            assertThat(closed.get(10, TimeUnit.SECONDS).getCode()).isEqualTo(1008);
        }
    }

    @Test
    void analyticsArchivesAValidatedKafkaEventWithTtlAndAcknowledgesADuplicateInDynamo() throws Exception {
        org.mockito.Mockito.when(dynamo.putItem(org.mockito.ArgumentMatchers.any(PutItemRequest.class)))
                .thenThrow(ConditionalCheckFailedException.builder().message("test duplicate").build());
        String payload = interaction(TENANT, A, 41, "VIEW", EventValidator.INTERACTION);
        String id = JSON.readTree(payload).get("eventId").asText();
        awaitCount(1);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(count("analytics_archive_pending")).isZero();
            org.mockito.Mockito.verify(dynamo).putItem(org.mockito.ArgumentMatchers.<PutItemRequest>argThat(request ->
                    request.tableName().equals("test-history") && request.conditionExpression().equals("attribute_not_exists(pk)")
                            && request.item().get("sk").s().equals(id)
                            && request.item().get("pk").s().equals(TENANT + "#" + A)
                            && Long.parseLong(request.item().get("expiresAt").n()) > Instant.now().getEpochSecond()));
        });
    }

    @Test
    @org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable(named="EV3_PRODUCER_CONTRACTS", matches="true")
    void actualProducerPayloadsReachKafkaProjectionAndRecommendationsWithoutTranslation() throws Exception {
        var products = JSON.readTree(java.nio.file.Files.readString(java.nio.file.Path.of(System.getenv("PRODUCTOS_CONTRACT_FILE"))));
        var cart = JSON.readTree(java.nio.file.Files.readString(java.nio.file.Path.of(System.getenv("CARRITO_CONTRACT_FILE"))));
        assertThat(products.size()).isEqualTo(2);
        assertThat(cart.size()).isEqualTo(3);
        var all = new java.util.ArrayList<com.fasterxml.jackson.databind.JsonNode>();
        products.forEach(all::add); cart.forEach(all::add);
        for (var event : all) {
            // Exact bytes/fields emitted by the producers: no translation or invented envelopes.
            for (int copy=0; copy<2; copy++) send(event.path("eventType").asText(), event.path("aggregateId").asText(), event.toString());
        }
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(count("event_inbox")).isEqualTo(5);
            assertThat(count("interaction")).isEqualTo(4);
            assertThat(count("product_projection")).isEqualTo(1);
        });
        var selection=products.get(1).path("data");
        var tenant=UUID.fromString(selection.path("tenantId").asText());
        var owner=UUID.fromString(selection.path("ownerId").asText());
        var result=recommendations.recommend(tenant, owner, 6);
        assertThat(result.estrategia()).isEqualTo("PERSONALIZED");
        assertThat(result.items()).extracting(RecommendationService.Item::productoId)
            .contains(selection.path("productoId").asLong());
        var cartData=cart.get(0).path("data");
        var summary=recommendations.summary(UUID.fromString(cartData.path("tenantId").asText()), UUID.fromString(cartData.path("ownerId").asText()));
        for (String action : List.of("ADD", "UPDATE", "REMOVE"))
            assertThat(((Map<?,?>) summary.get("porTipo")).get(action)).isEqualTo(1L);
        assertThat(count("event_quarantine")).isZero();
    }

    @Test void dynamicRecommendationsFallbacksEligibilityAndTenantIsolation() throws Exception {
        product(1, "Pizza", "food", 5, true, 1);
        product(2, "Drink", "drinks", 5, true, 1);
        product(3, "Toy", "toys", 5, true, 1);
        product(4, "Hidden", "food", 5, false, 1);
        product(5, "Empty", "food", 0, true, 1);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(count("product_projection")).isEqualTo(5));
        assertThat(recommendations.recommend(TENANT, A, 6).estrategia()).isEqualTo("FALLBACK_CATALOG");
        assertThat(recommendations.recommend(TENANT, A, 6).items()).extracting(RecommendationService.Item::productoId).containsExactly(1L, 2L, 3L);
        interaction(TENANT, B, 1, "ADD", EventValidator.CART);
        interaction(TENANT, B, 2, "ADD", EventValidator.CART);
        awaitCount(2);
        assertThat(recommendations.recommend(TENANT, A, 6).estrategia()).isEqualTo("FALLBACK_POPULARITY");
        String event = interaction(TENANT, A, 1, "VIEW", EventValidator.INTERACTION);
        send(EventValidator.INTERACTION, "1", event); // duplicate on the same or another partition is harmless
        interaction(OTHER_TENANT, A, 3, "ADD", EventValidator.CART);
        interaction(TENANT, A, 999, "SELECT", EventValidator.INTERACTION); // missing products remain excluded
        awaitCount(5);
        var result = recommendations.recommend(TENANT, A, 6);
        assertThat(result.estrategia()).isEqualTo("PERSONALIZED");
        assertThat(result.items()).extracting(RecommendationService.Item::productoId).containsExactly(1L, 2L, 3L);
        assertThat(result.items().get(1).score()).isPositive(); // collaborative affinity despite a different category
        assertThat(result.items().get(2).score()).isZero(); // different tenant cannot influence popularity
        mvc.perform(get("/api/analytics/me/resumen").header("Authorization", "Bearer " + token(TENANT, A, true)))
                .andExpect(status().isOk()).andExpect(jsonPath("$.totalInteracciones").value(2))
                .andExpect(jsonPath("$.porTipo.VIEW").value(1)).andExpect(jsonPath("$.porTipo.SELECT").value(1))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString(B.toString()))));
        interaction(TENANT, A, 3, "ADD", EventValidator.CART);
        awaitCount(6);
        assertThat(recommendations.recommend(TENANT, A, 1).items().getFirst().productoId()).isEqualTo(3);
        mvc.perform(get("/api/recomendaciones/me?limit=2").header("Authorization", "Bearer " + token(TENANT, A, true)))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(2))
                .andExpect(jsonPath("$.items[0].productoId").value(3)).andExpect(jsonPath("$.generadoEn").exists());
    }

    @Test void versionsAndInboxSurviveConsumerApplicationRestartAndRedelivery() throws Exception {
        String newest = envelope(EventValidator.PRODUCT, "7", 9, Map.of("productoId", 7, "nombre", "Newest", "categoria", "food", "precio", 12.5, "stock", 2, "activo", true));
        send(EventValidator.PRODUCT, "7", newest);
        String action = interaction(TENANT, A, 7, "ADD", EventValidator.CART);
        awaitCount(1);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(count("product_projection")).isEqualTo(1));
        CountDownLatch stopped = new CountDownLatch(1);
        listeners.stop(stopped::countDown);
        assertThat(stopped.await(15, TimeUnit.SECONDS)).isTrue();
        try (var restarted = new SpringApplicationBuilder(AnalyticsApplication.class).profiles("local").run(
                "--server.port=0", "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(), "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--LOCAL_JWT_JWK_SET_URI=http://127.0.0.1:" + JWKS.getAddress().getPort() + "/jwks")) {
            send(EventValidator.PRODUCT, "7", newest);
            send(EventValidator.CART, "7", action);
            product(7, "Old", "food", 0, false, 2);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(count("event_inbox")).isEqualTo(3));
            assertThat(count("interaction")).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT name FROM product_projection WHERE product_id=7", String.class)).isEqualTo("Newest");
            assertThat(restarted.getBean(RecommendationService.class).recommend(TENANT, A, 6).items()).hasSize(1);
        } finally { listeners.start(); }
    }

    @Test void invalidPayloadIsSanitizedAndDoesNotBlockPartition() throws Exception {
        String secret = "private.jwt.secret@example.test";
        try (var consumer = dltConsumer(EventValidator.INTERACTION)) {
            send(EventValidator.INTERACTION, secret, "{broken:" + secret);
            interaction(TENANT, A, 1, "VIEW", EventValidator.INTERACTION);
            awaitCount(1);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(count("event_quarantine")).isEqualTo(1));
            List<ConsumerRecord<String, String>> dlts = new ArrayList<>();
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(dlts::add);
                return !dlts.isEmpty();
            });
            assertThat(dlts.getFirst().value()).contains("INVALID_EVENT").doesNotContain(secret, "broken");
            assertThat(dlts.getFirst().key()).isNull();
            assertThat(dlts.getFirst().headers().toArray()).isEmpty();
        }
    }

    @Test void transientFailureRetriesBoundedlyAndRollsBackInboxWithProjection() throws Exception {
        jdbc.execute("CREATE SEQUENCE IF NOT EXISTS failure_attempts START 1");
        jdbc.execute("ALTER SEQUENCE failure_attempts RESTART WITH 1");
        jdbc.execute("""
            CREATE OR REPLACE FUNCTION fail_product() RETURNS trigger AS $$
            BEGIN IF NEW.product_id=999 THEN PERFORM nextval('failure_attempts'); RAISE EXCEPTION 'TEST_FAILURE'; END IF;
            RETURN NEW; END; $$ LANGUAGE plpgsql
            """);
        jdbc.execute("CREATE TRIGGER fail_test BEFORE INSERT ON product_projection FOR EACH ROW EXECUTE FUNCTION fail_product()");
        try {
            product(999, "Rejected", "test", 1, true, 1);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(count("event_quarantine")).isEqualTo(1));
            assertThat(jdbc.queryForObject("SELECT last_value FROM failure_attempts", Long.class)).isEqualTo(3);
            assertThat(count("event_inbox")).isZero();
            assertThat(count("product_projection")).isZero();
            assertThat(jdbc.queryForObject("SELECT reason FROM event_quarantine", String.class)).isEqualTo("PROCESSING_FAILED");
        } finally { jdbc.execute("DROP TRIGGER fail_test ON product_projection"); }
        product(999, "Recovered", "test", 1, true, 2);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(count("product_projection")).isEqualTo(1));
    }

    @Test void authenticationScopeAndInputAreFailClosed() throws Exception {
        mvc.perform(get("/api/recomendaciones/me")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/recomendaciones/me").header("Authorization", "Bearer " + token(TENANT, A, false))).andExpect(status().isForbidden());
        mvc.perform(get("/api/recomendaciones/me").header("Authorization", "Bearer " + token(OTHER_TENANT, A, true))).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/recomendaciones/me?limit=0").header("Authorization", "Bearer " + token(TENANT, A, true))).andExpect(status().isBadRequest());
        mvc.perform(get("/api/recomendaciones/me?limit=51").header("Authorization", "Bearer " + token(TENANT, A, true))).andExpect(status().isBadRequest());
        mvc.perform(post("/api/analytics/events").header("Authorization", "Bearer " + token(TENANT, A, true))).andExpect(status().isForbidden());
    }

    @Test void kafkaOffsetAdvancesOnlyAfterDatabaseCommit() throws Exception {
        jdbc.execute("CREATE SEQUENCE IF NOT EXISTS slow_attempts START 1");
        jdbc.execute("ALTER SEQUENCE slow_attempts RESTART WITH 1");
        jdbc.execute("""
            CREATE OR REPLACE FUNCTION slow_product() RETURNS trigger AS $$
            BEGIN IF NEW.product_id=888 THEN PERFORM nextval('slow_attempts'); PERFORM pg_sleep(4); END IF;
            RETURN NEW; END; $$ LANGUAGE plpgsql
            """);
        jdbc.execute("CREATE TRIGGER slow_test BEFORE INSERT ON product_projection FOR EACH ROW EXECUTE FUNCTION slow_product()");
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            String payload = envelope(EventValidator.PRODUCT, "888", 1, Map.of("productoId", 888, "nombre", "Slow", "categoria", "test", "precio", 1, "stock", 1, "activo", true));
            var metadata = kafka.send(EventValidator.PRODUCT, "888", payload).get(15, TimeUnit.SECONDS).getRecordMetadata();
            var partition = new TopicPartition(metadata.topic(), metadata.partition());
            await().atMost(Duration.ofSeconds(30)).until(() -> jdbc.queryForObject("SELECT is_called FROM slow_attempts", Boolean.class));
            assertThat(count("event_inbox")).isZero(); // uncommitted insert is invisible
            var before = admin.listConsumerGroupOffsets("ms-analytics-recommendations-v1").partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(partition);
            assertThat(before == null || before.offset() <= metadata.offset()).isTrue();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(count("event_inbox")).isEqualTo(1));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var after = admin.listConsumerGroupOffsets("ms-analytics-recommendations-v1").partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(partition);
                assertThat(after).isNotNull();
                assertThat(after.offset()).isGreaterThan(metadata.offset());
            });
        } finally { jdbc.execute("DROP TRIGGER slow_test ON product_projection"); }
    }

    private long count(String table) { return jdbc.queryForObject("SELECT count(*) FROM " + table, Long.class); }
    private void awaitCount(long count) { await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(count("interaction")).isEqualTo(count)); }
    private void product(long id, String name, String category, int stock, boolean active, long version) throws Exception {
        send(EventValidator.PRODUCT, Long.toString(id), envelope(EventValidator.PRODUCT, Long.toString(id), version,
                Map.of("productoId", id, "nombre", name, "categoria", category, "precio", 10.5, "stock", stock, "activo", active)));
    }
    private String interaction(UUID tenant, UUID owner, long id, String type, String topic) throws Exception {
        String event = envelope(topic, Long.toString(id), 1, Map.of("tenantId", tenant.toString(), "ownerId", owner.toString(), "productoId", id, "tipo", type));
        send(topic, Long.toString(id), event);
        return event;
    }
    private void send(String topic, String key, String payload) throws Exception { kafka.send(topic, key, payload).get(15, TimeUnit.SECONDS); }
    static String envelope(String topic, String aggregate, long version, Map<String, Object> data) throws Exception {
        return JSON.writeValueAsString(Map.of("eventId", UUID.randomUUID().toString(), "eventType", topic, "schemaVersion", 1,
                "occurredAt", Instant.now().toString(), "producer", topic.equals(EventValidator.CART) ? "ms-carrito" : "ms-productos",
                "aggregateId", aggregate, "aggregateVersion", version, "correlationId", UUID.randomUUID().toString(), "data", data));
    }
    static String token(UUID tenant, UUID owner, boolean scope) throws Exception {
        var claims = new JWTClaimsSet.Builder().issuer("https://login.microsoftonline.com/" + TENANT + "/v2.0")
                .audience(AUDIENCE).subject(owner.toString()).claim("tid", tenant.toString()).claim("oid", owner.toString())
                .claim("scp", scope ? "access_as_user" : "other").issueTime(new Date()).expirationTime(Date.from(Instant.now().plusSeconds(300))).build();
        var signed = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(KEY.getKeyID()).build(), claims);
        signed.sign(new RSASSASigner(KEY));
        return signed.serialize();
    }
    private KafkaConsumer<String, String> dltConsumer(String topic) {
        var consumer = new KafkaConsumer<String, String>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "group.id", UUID.randomUUID().toString(), "auto.offset.reset", "earliest", "enable.auto.commit", "false"),
                new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(topic + ".DLT"));
        return consumer;
    }
}
