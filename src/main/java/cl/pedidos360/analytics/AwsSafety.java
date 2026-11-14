package cl.pedidos360.analytics;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
@Profile("ev3-aws")
public class AwsSafety implements InitializingBean {
    private final Environment env;
    public AwsSafety(Environment env) { this.env = env; }
    @Override public void afterPropertiesSet() {
        if (env.matchesProfiles("local", "ev3-local", "test")
                || !env.getProperty("LOCAL_JWT_JWK_SET_URI", "").isBlank()
                || !env.getProperty("spring.security.oauth2.resourceserver.jwt.jwk-set-uri", "").isBlank()
                || !env.getProperty("spring.datasource.url", "").startsWith("jdbc:postgresql:"))
            throw new IllegalStateException("UNSAFE_EV3_AWS_CONFIGURATION");
    }
}
