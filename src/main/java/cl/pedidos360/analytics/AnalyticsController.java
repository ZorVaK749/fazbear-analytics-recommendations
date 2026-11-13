package cl.pedidos360.analytics;

import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class AnalyticsController {
    private final RecommendationService recommendations;
    public AnalyticsController(RecommendationService recommendations) { this.recommendations = recommendations; }

    @GetMapping("/health")
    Map<String, String> health() { return Map.of("status", "UP"); }

    @GetMapping("/api/recomendaciones/me")
    RecommendationService.Recommendations recommend(@AuthenticationPrincipal Jwt jwt, @RequestParam(defaultValue = "6") int limit) {
        if (limit < 1 || limit > 50) throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        return recommendations.recommend(UUID.fromString(jwt.getClaimAsString("tid")), UUID.fromString(jwt.getClaimAsString("oid")), limit);
    }

    @GetMapping("/api/analytics/me/resumen")
    Map<String, Object> summary(@AuthenticationPrincipal Jwt jwt) {
        return recommendations.summary(UUID.fromString(jwt.getClaimAsString("tid")), UUID.fromString(jwt.getClaimAsString("oid")));
    }
}
