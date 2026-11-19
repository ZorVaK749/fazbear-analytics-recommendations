package cl.pedidos360.analytics;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@Service
@ConditionalOnProperty(name = "analytics.dashboard.enabled", havingValue = "true")
class AnalyticsDashboard {
    private final JdbcTemplate db;
    private final URI socketUrl;
    private final SecureRandom entropy = new SecureRandom();

    AnalyticsDashboard(JdbcTemplate db, @Value("${analytics.dashboard.url}") URI socketUrl) {
        if (!"wss".equals(socketUrl.getScheme()) || socketUrl.getHost() == null
                || socketUrl.getQuery() != null || socketUrl.getUserInfo() != null
                || socketUrl.getFragment() != null || !"/ws/analytics".equals(socketUrl.getPath())) {
            throw new IllegalArgumentException("ANALYTICS_DASHBOARD_WSS_URL_REQUIRED");
        }
        this.db = db;
        this.socketUrl = socketUrl;
    }

    record Ticket(URI url, String ticket) {
        @Override public String toString() { return "Dashboard ticket"; }
    }
    record Lease(UUID tenant, UUID owner, Instant expiresAt) {}
    record Viewed(long productoId, String nombre, long visitas) {}
    record Category(String categoria, long interacciones) {}
    record Metrics(long interacciones, List<Viewed> productosMasVistos, List<Category> categorias) {}

    Ticket issue(Jwt jwt) {
        Instant now = Instant.now();
        Instant until = now.plusSeconds(300);
        if (jwt.getExpiresAt() == null || !jwt.getExpiresAt().isAfter(now))
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        if (jwt.getExpiresAt().isBefore(until)) until = jwt.getExpiresAt();
        Instant ticketExpiry = now.plusSeconds(30).isBefore(until) ? now.plusSeconds(30) : until;
        byte[] bytes = new byte[32];
        entropy.nextBytes(bytes);
        String ticket = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        db.update("INSERT INTO dashboard_ticket(digest,tenant_id,owner_id,expires_at,session_until) VALUES (?,?,?,?,?)",
                digest(ticket), UUID.fromString(jwt.getClaimAsString("tid")), UUID.fromString(jwt.getClaimAsString("oid")),
                ticketExpiry.atOffset(ZoneOffset.UTC), until.atOffset(ZoneOffset.UTC));
        return new Ticket(socketUrl, ticket);
    }

    Lease redeem(String ticket) {
        if (!ticket.matches("[A-Za-z0-9_-]{43}")) return null;
        var rows = db.query("""
                DELETE FROM dashboard_ticket WHERE digest=? AND expires_at > clock_timestamp()
                RETURNING tenant_id,owner_id,session_until
                """, (rs, row) -> new Lease(rs.getObject("tenant_id", UUID.class),
                rs.getObject("owner_id", UUID.class), rs.getObject("session_until", OffsetDateTime.class).toInstant()),
                digest(ticket));
        return rows.isEmpty() ? null : rows.getFirst();
    }

    void expireTickets() {
        db.update("DELETE FROM dashboard_ticket WHERE expires_at <= clock_timestamp()");
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public Metrics metrics(Lease lease) {
        OffsetDateTime cutoff = Instant.now().minusSeconds(3600).atOffset(ZoneOffset.UTC);
        Object[] scope = { lease.tenant(), lease.owner(), cutoff };
        long total = db.queryForObject("""
                SELECT count(*) FROM interaction WHERE tenant_id=? AND owner_id=? AND occurred_at >= ?
                """, Long.class, scope);
        var viewed = db.query("""
                SELECT i.product_id,coalesce(p.name,'Producto ' || i.product_id),count(*) AS visits
                FROM interaction i LEFT JOIN product_projection p USING (product_id)
                WHERE i.tenant_id=? AND i.owner_id=? AND i.occurred_at >= ? AND i.action='VIEW'
                GROUP BY i.product_id,p.name ORDER BY visits DESC,i.product_id LIMIT 5
                """, (rs, row) -> new Viewed(rs.getLong(1), rs.getString(2), rs.getLong(3)), scope);
        var categories = db.query("""
                SELECT coalesce(p.category,'Sin categoría'), count(*) AS total
                FROM interaction i LEFT JOIN product_projection p USING (product_id)
                WHERE i.tenant_id=? AND i.owner_id=? AND i.occurred_at >= ?
                GROUP BY 1 ORDER BY total DESC,1 LIMIT 8
                """, (rs, row) -> new Category(rs.getString(1), rs.getLong(2)), scope);
        return new Metrics(total, viewed, categories);
    }

    private static String digest(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.US_ASCII)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable");
        }
    }
}

@RestController
@ConditionalOnProperty(name = "analytics.dashboard.enabled", havingValue = "true")
class DashboardTickets {
    private final AnalyticsDashboard dashboard;

    DashboardTickets(AnalyticsDashboard dashboard) { this.dashboard = dashboard; }

    @PostMapping("/api/analytics/me/dashboard-ticket")
    ResponseEntity<AnalyticsDashboard.Ticket> ticket(@AuthenticationPrincipal Jwt jwt) {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(dashboard.issue(jwt));
    }
}
