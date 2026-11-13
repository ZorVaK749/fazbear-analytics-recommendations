package cl.pedidos360.analytics;

import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.security.oauth2.jwt.Jwt;

import static org.assertj.core.api.Assertions.*;

class SecurityConfigTest {
    final String tenant = "ee0d2c5b-e60b-4c57-9b30-8a38a520c430";
    final String audience = "analytics-api";

    @Test void localJwksRequiresExplicitProfileAndDoesNotDisableAzureClaimsValidation() {
        var environment = new MockEnvironment().withProperty("LOCAL_JWT_JWK_SET_URI", "http://localhost:9999/jwks");
        assertThatThrownBy(() -> new SecurityConfig().jwtDecoder(tenant, audience, environment))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("explicit local profile");
        environment.setActiveProfiles("local");
        assertThat(new SecurityConfig().jwtDecoder(tenant, audience, environment)).isNotNull();
        environment.setProperty("LOCAL_JWT_JWK_SET_URI", "file:///keys.json");
        assertThatThrownBy(() -> new SecurityConfig().jwtDecoder(tenant, audience, environment)).isInstanceOf(IllegalStateException.class);
    }

    @Test void validatesIssuerAudienceExpiryTenantAndCanonicalOid() {
        var base = new HashMap<String, Object>();
        base.put("tid", tenant);
        base.put("oid", UUID.randomUUID().toString());
        base.put("aud", List.of(audience));
        base.put("iss", "https://login.microsoftonline.com/" + tenant + "/v2.0");
        base.put("sub", "pseudonymous-subject");
        base.put("exp", Instant.now().plusSeconds(300));
        var validator = SecurityConfig.validators(tenant, audience);
        assertThat(validator.validate(jwt(base)).hasErrors()).isFalse();
        for (var mutation : List.<Map<String, Object>>of(Map.of("tid", UUID.randomUUID().toString()), Map.of("oid", "1-1-1-1-1"),
                Map.of("aud", List.of("wrong")), Map.of("iss", "http://local"), Map.of("exp", Instant.now().minusSeconds(300)))) {
            var claims = new HashMap<>(base);
            claims.putAll(mutation);
            assertThat(validator.validate(jwt(claims)).hasErrors()).isTrue();
        }
        for (String required : List.of("oid", "tid", "exp", "sub")) {
            var claims = new HashMap<>(base);
            claims.remove(required);
            assertThat(validator.validate(jwt(claims)).hasErrors()).isTrue();
        }
    }

    private Jwt jwt(Map<String, Object> claims) {
        return Jwt.withTokenValue("not-logged").header("alg", "RS256").claims(c -> c.putAll(claims)).build();
    }
}
