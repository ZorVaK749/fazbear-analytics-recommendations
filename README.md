# ms-analytics-recommendations — PEDIDOS360 EV3

Diseño de analítica y recomendaciones basadas en eventos Kafka de Productos y
Carrito y catálogo real Fazbear. **Sin lógica implementada ni despliegue.**

- `docs/ARQUITECTURA.md`: propuesta de responsabilidad, API/eventos y seguridad.
- `src/README.md`: estructura futura.
- `tests/README.md`: criterios, no evidencia de pruebas ejecutadas.
- `deploy/README.md`: propuesta Docker/CI/AWS.

Java 21, Spring Boot, PostgreSQL y Kafka propuestos; no pom.xml ni imagen todavía.
RabbitMQ mantiene el procesamiento operativo. No reemplazarlo con Kafka ni usar
los experimentos históricos de sensores como estos servicios.

Diseño transversal en workspace: `docs/ARQUITECTURA_NUEVOS_SERVICIOS_EV3.md`.
Contratos e infraestructura pendientes de aprobación.
