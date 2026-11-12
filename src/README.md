# Estructura futura, no aplicación ejecutable

`src/main/java/.../analytics/`: api, application, domain (ranking/afinidad),
infrastructure (Kafka/inbox, JWT, catálogo y persistencia).
Migraciones en `src/main/resources/db/migration`; tests en src/test.
No datos ficticios como recomendaciones de producción.
