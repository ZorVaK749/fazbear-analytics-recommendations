# Suite ejecutable — integración integral

`mvn verify` usa PostgreSQL y Kafka reales con Testcontainers. Incluye eventos
duplicados/versiones, caída de persistencia, retry/DLT saneada, commit antes del
offset, reinicio de aplicación, ranking dinámico, fallback y autenticación RSA.
Prueba explícita REMOVE con cantidad resultante cero, coherente con Carrito.
Conteos finales en informe consolidado de `fazbear-pago/docs/Informe_Fazbear_EV3.md`.

JUnit/Spring Security/Testcontainers PostgreSQL y Kafka: contratos de eventos
reales de Productos/Carrito, duplicados, desorden/versiones, crash entre BD/offset,
broker caído, DLT y replay autorizado. Ranking determinista con recencia/afinidad;
usuario nuevo/fallback, stock agotado/inactivo/desconocido/catálogo obsoleto.
JWT/scopes, ownership A/B y límites. Roles admin/retención/antiabuso productivos
siguen pendientes; no se declaran certificados por esta suite.
No confundir tests locales con evidencia de AWS o datos reales ya integrados.
