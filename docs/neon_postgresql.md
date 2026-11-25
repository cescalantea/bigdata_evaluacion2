# Neon PostgreSQL — Grupo 4

Rama: `jose/conexion_neon_postgresql`. Base: `c7fe2cdd62be791bafb6dd6e0b742aeb35901661` de `origin/Conexion_mongo`, tras `git fetch origin`.

## Configurar y ejecutar en PowerShell

```powershell
python -m pip install -r requirements.txt
Copy-Item .env.example .env
# Editar .env localmente, manteniendo las variables MongoDB.
python servidor_prueba.py
```

En Neon, seleccionar BigData_Socket / production / neondb y copiar la URL de conexion a `DATABASE_URL` en `.env`. Debe incluir SSL (`sslmode=require`, o verificacion de certificado). Codificar caracteres especiales de usuario/contrasena en URL. No publicar este archivo; `.gitignore` ya lo excluye. URL vacia deshabilita SQL. Las tablas existentes deben estar en `public` y el usuario necesita SELECT, INSERT, acceso a la secuencia identity y permiso para tablas temporales. No hay migraciones permanentes.

`POSTGRES_BATCH_SIZE=10000` controla el recorrido de filas COPY; no produce INSERT individuales ni COMMIT por lote. `POSTGRES_CONNECT_TIMEOUT=15` limita la conexion y `POSTGRES_STATEMENT_TIMEOUT_MS=300000` cada sentencia SQL y espera por bloqueos. No constituyen un limite total de carga.

## Flujo

El trabajador TCP original recibe JSON terminado en `<EOF>`, valida el mensaje y construye un unico DataFrame. Inicia MongoDB y PostgreSQL en paralelo, conserva resultados y duraciones en `_ultima_persistencia`, actualiza la GUI mediante su cola y envia ACK de recepcion. No se cambian cliente, puertos, sensores ni JSON entrante. La GUI diferencia recepcion completada de persistencia finalizada.

`SQLRepositorio` carga dotenv y valida configuracion. Antes de conectar valida exactamente las ocho columnas, fechas ISO, INTEGER, textos, coordenadas, valores finitos, precision/escala NUMERIC, caudales y areas no negativos. Usa Decimal desde la representacion recibida: no recupera precision que el cliente ya haya perdido al leer CSV. Rechaza redondeos implicitos, metadatos contradictorios y valores distintos para la misma clave de medicion. Colapsa duplicados identicos del propio envio.

En una conexion por carga, COPY envia estaciones unicas y mediciones a dos tablas temporales. Un bloqueo asesor transaccional serializa las comprobaciones entre instancias de este repositorio. Comprueba contradicciones con datos existentes, inserta estaciones y despues mediciones con ON CONFLICT DO NOTHING. El contexto psycopg confirma ambas tablas juntas o revierte ambas. Cuenta inserciones efectivas con rowcount y separa mediciones existentes. Cierra recursos incluso ante errores. Escritores externos que no respeten el bloqueo asesor requieren coordinacion adicional.

Referencia del mecanismo COPY y transacciones: https://www.psycopg.org/psycopg3/docs/basic/copy.html y https://www.psycopg.org/psycopg3/docs/basic/transactions.html.

## Estados parciales y reintentos

No existe transaccion distribuida: Mongo puede confirmar y SQL fallar, SQL puede confirmar y Mongo fallar, o las escrituras pueden confirmar antes de fallar el dashboard/ACK. Un corte durante COMMIT puede dejar resultado desconocido; consultar conteos antes de decidir. El fallo Mongo puede dejar lotes parciales, por lo que cero insertados en un resultado fallido significa cero confirmados por esta capa, no garantiza ausencia de documentos.

SQL admite reenvios identicos sin duplicar y rechaza cambios de contenido para claves existentes. No se realizan reintentos automaticos del envio completo. MongoRepositorio se conserva y sus insert_many originales NO son idempotentes: reenviar el CSV completo puede duplicar documentos Mongo. Para recuperar solamente SQL, usar SQLRepositorio.guardar_dataframe con el DataFrame CAMELS, sin volver a enviar por TCP. Para recuperar Mongo, reconciliar primero los documentos ya almacenados; no reintentar a ciegas. Hacer idempotente Mongo requeriria acordar identidad e indices con el equipo.

## Verificacion SQL (solo lectura)

```sql
SELECT count(*) AS estaciones FROM public.estaciones;
SELECT count(*) AS mediciones FROM public.mediciones_caudal;
SELECT count(DISTINCT estacion_id) AS estaciones_con_medicion,
       min(fecha), max(fecha) FROM public.mediciones_caudal;
SELECT estacion_id, fecha, count(*) FROM public.mediciones_caudal
GROUP BY estacion_id, fecha HAVING count(*) > 1;
SELECT count(*) AS huerfanas FROM public.mediciones_caudal m
LEFT JOIN public.estaciones e USING (estacion_id) WHERE e.estacion_id IS NULL;
```

En una base inicialmente vacia, solo despues de cargar y confirmar el CSV real completo se esperan 414 estaciones y 569250 mediciones. En una base con datos anteriores, comparar tambien los contadores devueltos y las claves del dataset; los conteos globales no prueban por si solos que una carga especifica llego completa.

## Pruebas y rendimiento

`python -m pytest -q --disable-warnings` ejecuta pruebas anteriores y nuevas sin credenciales. Los tests nuevos usan datos CAMELS sinteticos, un doble transaccional SQL, conexiones simuladas y sockets reales con Tkinter. No prueban el motor PostgreSQL real mediante mocks. Para una comprobacion Neon separada y exclusivamente de lectura, configurar `NEON_TEST_DATABASE_URL` y ejecutar `python -m pytest tests/test_sql_repositorio.py -k neon_real -q`. No se hace carga automatica a produccion.

No se dispone del CSV real ni de credenciales Neon. No se ha medido persistencia remota ni se garantiza completar 569250 filas en los 60 segundos de espera del cliente. Mongo y SQL se ejecutan secuencialmente y sus tiempos se suman. La prueba masiva antigua mide transferencia/GUI sintetica con destinos deshabilitados, no Neon. Para medir Neon, cronometrar una carga autorizada en base de pruebas y un reenvio, incluyendo validacion y Mongo. Si excede 60 s, una solucion compatible con el framing actual es aumentar TIEMPO_RESPUESTA del cliente y el plazo de aceptacion del servidor de forma coordinada, despues de medir. No enviar un ACK anticipado que afirme persistencia. No se modifica cliente.py sin esa evidencia.

Resultado final de la suite en Windows: **91 passed, 5 skipped, 1 warning en 42.61 s**. Las omisiones incluyen el CSV real ausente, Neon sin credenciales y pruebas condicionales anteriores. Se ejecuto con TK_LIBRARY apuntando a la instalacion local tk8.6, tras un fallo de descubrimiento de tk.tcl al ejecutar pruebas por separado. Tambien pasaron py_compile y git diff --check.
