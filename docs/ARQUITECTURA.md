# Diseño propuesto — ms-analytics-recommendations

> Histórico de diseño. Implementación integral disponible en src/main y contrato
> actual en README raíz. API admin y otros contratos no descritos allí siguen pendientes.

## Responsabilidad y tecnología

Proyectar comportamiento, afinidades y recomendaciones de catálogo real. No pagos,
stock operativo, pedidos o saldos. Java 21/Spring Boot 3.x soportado por fijar,
Spring Kafka, Resource Server Azure JWT y PostgreSQL/Flyway.
Tablas: interaction, consumer_inbox, product_projection, user_affinity,
recommendation_snapshot y outbox_event cuando produzca eventos propios.

## REST sugerido (no implementado)

- GET `/api/recomendaciones/me?limit=10`: items productoId/razon, generadoEn,
  estrategia y catalogoVersion. Limit 1..20, propietario desde JWT.
- GET `/api/analytics/me/resumen`: actividad propia.
- GET `/api/analytics/admin/metricas?desde=...&hasta=...`: rol Analytics.Read y rango limitado.

JWT verifica firma/issuer/audience/exp/nbf/tenant, scope delegado o rol interno;
sin token → 401, sin permiso → 403, datos ajenos no expuestos. Mapeo `(tid,oid)`
a identidad histórica pendiente de comprobar; no usuarioId/email libre de Angular.
Sin endpoint de publicación arbitraria de eventos. Interacciones UI requieren
futuro endpoint validado de Productos y cambios autorizados en productores.

## Kafka y recomendación

Topics sugeridos: producto.interaccion.v1, producto.actualizado.v1,
carrito.actualizado.v1; opcional pago.aprobado.v1 sin datos de tarjeta.
Productos/Carrito son fuentes autorizadas de eventos con identidad validada y
producto real. Productores todavía no implementados ni autorizados a modificarse.

Envelope: eventId, eventType, schemaVersion, occurredAt UTC, producer,
aggregateId/version, correlationId y data. Key userKey pseudónimo en interacción/
carrito; productoId en catálogo. Data: productoId/tipo/sesión pseudónima o
carritoId/acción/cantidad resultante/versión; catálogo categoría/activo/disponibilidad.
Pseudónimo no equivale a anonimato; sin JWT/email/IP/huellas de tarjeta.

Inbox única `(consumer,eventId)` y proyección en misma transacción PostgreSQL;
offset después de commit. Outbox en productores, al menos una vez; deduplicar
sin contar dos veces, versiones para catálogo/carrito y tolerar desorden temporal.
Backoff, DLT y replay manual auditado. Kafka no reemplaza RabbitMQ pedido.creado v2.

Algoritmo inicial determinista: pesos configurables por interacción reciente,
afinidad de categorías, popularidad para usuario nuevo. Excluir productos no
existentes/inactivos/sin stock; proyección obsoleta requiere revalidación Productos
o exclusión explícita. No inventar precios/productos ni exigir ML externo.

## Seguridad, privacidad y aceptación

ACL Kafka por productor/grupo/topic, APIs con límites y scopes, métricas de lag;
rate limit para abuso de clicks. Retención propuesta 30 días de eventos crudos,
pendiente de aprobación y política de borrado/acceso/finalidad aplicable.
Pruebas y despliegue en guías separadas. No hay tests ejecutables, imagen ni E2E.
