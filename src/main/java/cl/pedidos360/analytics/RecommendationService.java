package cl.pedidos360.analytics;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class RecommendationService {
    private final JdbcTemplate jdbc;
    public RecommendationService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public record Item(long productoId, String nombre, String categoria, BigDecimal precio, BigDecimal score, String razon) {}
    public record Recommendations(String estrategia, List<Item> items, Instant generadoEn) {}
    private record Candidate(long id, String name, String category, BigDecimal price, double personal, double popularity) {}

    @Transactional(readOnly = true)
    public Recommendations recommend(UUID tenant, UUID owner, int limit) {
        Instant now = Instant.now();
        List<Candidate> candidates = jdbc.query("""
            WITH weighted AS (
                SELECT owner_id, product_id,
                  sum((CASE action WHEN 'VIEW' THEN 1 WHEN 'SELECT' THEN 2 WHEN 'ADD' THEN 5
                       WHEN 'UPDATE' THEN 3 WHEN 'REMOVE' THEN -5 END)
                       / (1 + greatest(0, extract(epoch from (?::timestamptz - occurred_at))) / 604800.0)) AS strength
                FROM interaction WHERE tenant_id=? AND occurred_at >= ?::timestamptz - interval '90 days'
                GROUP BY owner_id,product_id
            ), interests AS (
                SELECT product_id,strength FROM weighted WHERE owner_id=? AND strength>0
            ), categories AS (
                SELECT p.category,sum(i.strength) AS strength FROM interests i
                JOIN product_projection p USING(product_id) GROUP BY p.category
            ), affinities AS (
                SELECT candidate.product_id, sum(least(seed.strength,candidate.strength)*i.strength) AS strength
                FROM interests i JOIN weighted seed ON seed.product_id=i.product_id AND seed.strength>0
                JOIN weighted candidate ON candidate.owner_id=seed.owner_id AND candidate.product_id<>seed.product_id
                  AND candidate.strength>0
                WHERE seed.owner_id<>? GROUP BY candidate.product_id
            ), popularity AS (
                SELECT product_id,sum(greatest(0,strength)) AS strength FROM weighted GROUP BY product_id
            )
            SELECT p.*, coalesce(i.strength,0) + 0.25*coalesce(c.strength,0) + 0.1*coalesce(a.strength,0) AS personal,
                   coalesce(pop.strength,0) AS popularity
            FROM product_projection p LEFT JOIN interests i USING(product_id)
            LEFT JOIN categories c ON c.category=p.category LEFT JOIN affinities a USING(product_id)
            LEFT JOIN popularity pop USING(product_id)
            WHERE p.active=true AND p.stock>0
            ORDER BY personal DESC,popularity DESC,p.product_id ASC LIMIT ?
            """, (rs, row) -> new Candidate(rs.getLong("product_id"), rs.getString("name"), rs.getString("category"),
                rs.getBigDecimal("price"), rs.getDouble("personal"), rs.getDouble("popularity")),
                Timestamp.from(now), tenant, Timestamp.from(now), owner, owner, limit);
        String strategy = candidates.stream().anyMatch(c -> c.personal > 0) ? "PERSONALIZED"
                : candidates.stream().anyMatch(c -> c.popularity > 0) ? "FALLBACK_POPULARITY" : "FALLBACK_CATALOG";
        List<Item> items = candidates.stream().map(c -> new Item(c.id, c.name, c.category, c.price,
                BigDecimal.valueOf(c.personal > 0 ? c.personal : c.popularity).setScale(6, RoundingMode.HALF_UP),
                c.personal > 0 ? "INTERESES_Y_AFINIDAD" : c.popularity > 0 ? "POPULARIDAD_TENANT" : "CATALOGO_DISPONIBLE")).toList();
        return new Recommendations(strategy, items, now);
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public Map<String, Object> summary(UUID tenant, UUID owner) {
        var counts = new TreeMap<String, Long>();
        for (String action : List.of("VIEW", "SELECT", "ADD", "UPDATE", "REMOVE")) counts.put(action, 0L);
        jdbc.query("SELECT action,count(*) AS total FROM interaction WHERE tenant_id=? AND owner_id=? GROUP BY action",
                rs -> { counts.put(rs.getString("action"), rs.getLong("total")); }, tenant, owner);
        var totals = jdbc.queryForMap("""
            SELECT count(*) AS total, count(DISTINCT product_id) AS products,max(occurred_at) AS latest
            FROM interaction WHERE tenant_id=? AND owner_id=?
            """, tenant, owner);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("totalInteracciones", totals.get("total"));
        result.put("productosDistintos", totals.get("products"));
        result.put("porTipo", counts);
        result.put("ultimaInteraccion", totals.get("latest") == null ? null : ((Timestamp) totals.get("latest")).toInstant());
        return result;
    }
}
