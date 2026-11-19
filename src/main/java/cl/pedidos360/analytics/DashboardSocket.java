package cl.pedidos360.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

class DashboardSocket extends TextWebSocketHandler {
    private final Map<String, Connection> clients = new ConcurrentHashMap<>();
    private final Semaphore capacity = new Semaphore(100);
    private final AnalyticsDashboard dashboard;
    private final ObjectMapper json;

    DashboardSocket(AnalyticsDashboard dashboard, ObjectMapper json) {
        this.dashboard = dashboard;
        this.json = json;
    }

    private static final class Connection {
        private final WebSocketSession session;
        private final Instant authenticateBy = Instant.now().plusSeconds(5);
        private volatile AnalyticsDashboard.Lease lease;
        private AnalyticsDashboard.Metrics previous;
        private int idleTicks;

        Connection(WebSocketSession session) {
            this.session = new ConcurrentWebSocketSessionDecorator(session, 2000, 16384);
        }
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws IOException {
        if (!capacity.tryAcquire()) {
            session.close(CloseStatus.SERVICE_OVERLOAD);
            return;
        }
        session.setTextMessageSizeLimit(128);
        clients.put(session.getId(), new Connection(session));
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
        Connection client = clients.get(session.getId());
        if (client == null || client.lease != null || Instant.now().isAfter(client.authenticateBy)) {
            session.close(CloseStatus.POLICY_VIOLATION);
            return;
        }
        try {
            client.lease = dashboard.redeem(message.getPayload());
            if (client.lease == null) session.close(CloseStatus.POLICY_VIOLATION);
        } catch (RuntimeException unavailable) {
            session.close(CloseStatus.SERVER_ERROR);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        if (clients.remove(session.getId()) != null) capacity.release();
    }

    @Scheduled(fixedDelay = 2500)
    void publishMetrics() {
        var now = Instant.now();
        for (Connection client : clients.values()) {
            try {
                if (client.lease == null) {
                    if (now.isAfter(client.authenticateBy)) client.session.close(CloseStatus.POLICY_VIOLATION);
                    continue;
                }
                if (now.isAfter(client.lease.expiresAt())) {
                    client.session.close(CloseStatus.POLICY_VIOLATION);
                    continue;
                }
                var metrics = dashboard.metrics(client.lease);
                if (!metrics.equals(client.previous) || ++client.idleTicks >= 8) {
                    client.session.sendMessage(new TextMessage(json.writeValueAsString(metrics)));
                    client.previous = metrics;
                    client.idleTicks = 0;
                }
            } catch (IOException | RuntimeException unavailable) {
                try { client.session.close(CloseStatus.SERVER_ERROR); }
                catch (IOException ignored) { afterConnectionClosed(client.session, CloseStatus.SERVER_ERROR); }
            }
        }
    }

    @Scheduled(fixedDelay = 60000)
    void cleanTickets() { dashboard.expireTickets(); }

    @Configuration
    @EnableWebSocket
    @EnableScheduling
    @ConditionalOnProperty(name = "analytics.dashboard.enabled", havingValue = "true")
    static class SocketConfig {
        @Bean
        DashboardSocket analyticsSocket(AnalyticsDashboard dashboard, ObjectMapper mapper) {
            return new DashboardSocket(dashboard, mapper);
        }

        @Bean
        WebSocketConfigurer dashboardSocketMapping(DashboardSocket socket, @Value("${analytics.dashboard.origin}") URI origin) {
            if (!"https".equals(origin.getScheme()) || origin.getHost() == null || origin.getUserInfo() != null
                    || !origin.getPath().isEmpty() || origin.getQuery() != null || origin.getFragment() != null)
                throw new IllegalArgumentException("ANALYTICS_DASHBOARD_ORIGIN_REQUIRED");
            return registry -> registry.addHandler(socket, "/ws/analytics").setAllowedOrigins(origin.toString());
        }
    }
}
