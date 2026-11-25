# Informe de integración de PostgreSQL Neon al servidor Big Data

**Grupo 4 · 21 de noviembre de 2026**

## 1. Objetivo y cambios realizados

Se incorporó la persistencia relacional PostgreSQL, alojada en Neon, al servidor Python existente. El servidor ahora puede enviar el mismo DataFrame recibido por TCP a MongoDB Atlas y a PostgreSQL. La integración utiliza el servidor y el trabajador de recepción originales; las operaciones de base de datos se realizan fuera del hilo gráfico de Tkinter.

El trabajo se implementó en la rama `jose/conexion_neon_postgresql`, desde el commit base `c7fe2cdd62be791bafb6dd6e0b742aeb35901661` de `Conexion_mongo`. El commit de implementación es `46847fe`.

| Archivo | Cambio y propósito |
|---|---|
| `sql_repositorio.py` | Nuevo repositorio SQL: configuración, conexión, validación y carga transaccional. |
| `servidor_prueba.py` | Inicializa SQLRepositorio, coordina los dos destinos, muestra resultados y cierra recursos SQL. |
| `requirements.txt` | Agrega `psycopg[binary]>=3.2,<4`; reutiliza `python-dotenv`. |
| `.env.example` | Agrega las variables necesarias para configurar PostgreSQL. |
| `tests/test_sql_repositorio.py` | Pruebas de validación, carga, duplicados, rollback, conexión e integración TCP. |
| `docs/neon_postgresql.md` | Guía de configuración, consultas y recuperación ante resultados parciales. |

La implementación MongoDB se conserva. El cliente sigue enviando el JSON terminado en `<EOF>` por el puerto 12345; el puerto de sensores continúa siendo el 12346.

## 2. Cómo se integró al servidor

En `ServidorBigData.__init__()` se crea `self.sql = SQLRepositorio()`, junto al repositorio MongoDB existente.

En `_atender_cliente()`, después de recibir y validar el JSON y construir `df_nuevo`, se llama a `_persistir_dataframe(df_nuevo)`. Este método inicia en paralelo la persistencia en MongoDB y PostgreSQL. Cada destino tiene manejo de errores y medición de tiempo independiente: si una base falla, la otra se intenta igualmente.

```text
Cliente: CSV → JSON + <EOF>
                 ↓
Servidor TCP: recepción y validación del mensaje
                 ↓
             df_nuevo
                 ↓
     _persistir_dataframe(df_nuevo)
                 ├─ MongoRepositorio → MongoDB Atlas
                 └─ SQLRepositorio   → PostgreSQL Neon
                 ↓
Resultados por destino → cola de Tkinter → dashboard → ACK
```

Los resultados se conservan en `_ultima_persistencia`. La actualización visual se envía a la cola de Tkinter para que la GUI la aplique desde su propio hilo. Al cerrar el servidor también se llama a `self.sql.cerrar()`.

## 3. Organización de los datos relacionales

El CSV contiene datos de una estación repetidos en cada fecha. PostgreSQL separa esa información en las dos tablas existentes:

| Tabla | Información almacenada | Clave |
|---|---|---|
| `public.estaciones` | Identificador, nombre, latitud, longitud, zona y área de cuenca. | `estacion_id` |
| `public.mediciones_caudal` | Estación, fecha y caudal; el identificador de medición se genera automáticamente. | `medicion_id`, con unicidad en `(estacion_id, fecha)` |

Una estación puede tener muchas mediciones. Por ejemplo, dos filas del CSV correspondientes a la misma estación en fechas diferentes producen una estación y dos mediciones. Esta separación evita repetir los metadatos de la estación en cada medición.

## 4. Funcionamiento de SQLRepositorio

### Configuración y conexión

`__init__()` carga las variables mediante `python-dotenv`, comprueba los límites configurados y valida la URL. La propiedad `habilitado` indica si existe `DATABASE_URL`.

`conectar()` utiliza psycopg versión 3, exige SSL y aplica límites para la conexión, las sentencias y las esperas por bloqueos. Los mensajes propios de error evitan incluir credenciales o detalles del driver.

### Validación antes de escribir

`preparar()` exige exactamente estas ocho columnas, sin nombres duplicados:

```text
fecha, estacion_id, estacion_nombre, latitud, longitud,
zona_latitudinal, area_cuenca_km2, caudal_m3_s
```

Comprueba fechas válidas, identificadores enteros dentro del rango PostgreSQL, textos y sus longitudes, coordenadas geográficas, números finitos, áreas y caudales no negativos. También verifica que los valores quepan en la precisión y escala de las columnas NUMERIC.

Los números se transforman mediante `Decimal` a partir del valor recibido. Se rechazan valores que exigirían redondeo al guardarse; esta conversión no recupera precisión que el cliente haya perdido previamente.

Durante la separación de datos se detectan estaciones con metadatos contradictorios y mediciones con caudales distintos para una misma estación y fecha. Los duplicados idénticos del envío se agrupan.

Si faltan columnas o el conjunto recibido no coincide con el esquema esperado, SQL devuelve estado `omitido` y una explicación. MongoDB y la recepción siguen funcionando. Los demás errores de validación se registran como un fallo del destino PostgreSQL en la coordinación del servidor.

### Carga masiva y transacción

`guardar_dataframe()` ejecuta los siguientes pasos:

1. Valida y separa estaciones únicas y mediciones antes de abrir la conexión.
2. Abre una conexión para la carga y crea dos tablas temporales: `carga_estaciones` y `carga_mediciones`.
3. Transfiere los datos mediante `COPY FROM STDIN`, recorriendo grupos según `POSTGRES_BATCH_SIZE`.
4. Adquiere un bloqueo asesor transaccional para coordinar las cargas de este repositorio.
5. Compara los datos temporales con los existentes y rechaza contradicciones.
6. Inserta primero las estaciones y luego las mediciones para respetar la clave foránea.
7. Utiliza `ON CONFLICT DO NOTHING` para omitir claves ya almacenadas con el mismo contenido.
8. Confirma la transacción y devuelve los contadores. Ante un error previo al COMMIT, el contexto de conexión revierte la transacción.
9. Cierra la conexión y libera recursos.

Las tablas temporales se eliminan al confirmar la transacción. No se recrean ni alteran las tablas permanentes. COPY transmite filas a través de su protocolo masivo; no se ejecuta un INSERT independiente por registro. El tamaño de lote tampoco significa un COMMIT por lote: la carga SQL completa pertenece a una sola transacción.

Un resultado SQL confirmado informa `procesados`, `insertados`, `estaciones_insertadas` y `existentes`. `insertados` cuenta mediciones nuevas; `existentes` cuenta mediciones únicas del envío que ya estaban almacenadas. Los contadores se devuelven como éxito después de salir del contexto transaccional.

## 5. Respuestas y resultados parciales

| Situación | Comportamiento |
|---|---|
| DATABASE_URL vacía | SQL queda deshabilitado; continúa la recepción existente. |
| Columnas incompatibles con CAMELS | Se omite SQL y se informa la causa; MongoDB se intenta normalmente. |
| Carga SQL válida | Se insertan estaciones y mediciones y se muestran los contadores. |
| Reenvío idéntico a SQL | No se duplican claves ya almacenadas. |
| Una base falla | Se informa el estado de ambas bases y se devuelve ACK negativo. |

El ACK positivo mantiene el contrato existente:

```json
{"ok": true, "filas": 2}
```

Este ACK confirma recepción y aceptación; también puede emitirse cuando SQL está deshabilitado u omitido. No demuestra por sí solo que hubo persistencia SQL: para eso deben revisarse el resultado del destino y los datos almacenados.

En un fallo de persistencia, la respuesta incluye `ok: false`, las filas recibidas, un detalle y los resultados por base. El dashboard puede mostrar datos recibidos aunque una escritura haya fallado.

MongoDB y PostgreSQL no comparten una transacción distribuida. Una puede confirmar mientras la otra falla; también puede fallar la GUI o perderse el ACK después de guardar. Si se interrumpe la conexión durante COMMIT, el resultado puede ser desconocido y debe comprobarse antes de reintentar.

La carga SQL es idempotente para datos idénticos. MongoDB conserva su `insert_many` original y puede duplicar documentos al reenviar todo el CSV. No se agregaron reintentos automáticos del envío completo. Para recuperar solo SQL se puede invocar `SQLRepositorio.guardar_dataframe()` con el DataFrame correspondiente, evitando volver a escribir en MongoDB.

## 6. Configuración y ejecución

Crear `.env` desde el ejemplo solo si aún no existe y completar localmente la URL obtenida de Neon para el proyecto `BigData_Socket`, rama `production`, base `neondb`:

```dotenv
DATABASE_URL=postgresql://usuario:password@host.neon.tech/neondb?sslmode=require
POSTGRES_BATCH_SIZE=10000
POSTGRES_CONNECT_TIMEOUT=15
POSTGRES_STATEMENT_TIMEOUT_MS=300000
```

Los valores de la URL anterior son ficticios. El archivo `.env` está excluido por `.gitignore`. Deben conservarse las variables MongoDB necesarias para usar ambos destinos.

El usuario PostgreSQL necesita permisos de lectura e inserción, acceso a la secuencia identity y creación de tablas temporales. Las tablas deben existir en el esquema `public`.

Desde la carpeta del proyecto:

```powershell
python -m pip install -r requirements.txt
python servidor_prueba.py
```

## 7. Comprobación de los datos

Ejecutar en el editor SQL de Neon:

```sql
-- Cantidades almacenadas.
SELECT count(*) AS estaciones FROM public.estaciones;
SELECT count(*) AS mediciones FROM public.mediciones_caudal;

-- Resumen de fechas y estaciones con mediciones.
SELECT count(DISTINCT estacion_id) AS estaciones_con_medicion,
       min(fecha) AS primera_fecha,
       max(fecha) AS ultima_fecha
FROM public.mediciones_caudal;

-- Debe devolver cero filas por la restricción UNIQUE.
SELECT estacion_id, fecha, count(*)
FROM public.mediciones_caudal
GROUP BY estacion_id, fecha
HAVING count(*) > 1;

-- Ejemplo de consulta que reúne ambas tablas.
SELECT e.estacion_id, e.estacion_nombre, m.fecha, m.caudal_m3_s
FROM public.estaciones e
JOIN public.mediciones_caudal m USING (estacion_id)
ORDER BY e.estacion_id, m.fecha
LIMIT 20;
```

Según las características declaradas del CSV real, una base inicialmente vacía debería quedar con 414 estaciones y 569250 mediciones después de una carga completa confirmada. Estos valores son expectativas del dataset, no resultados medidos de una carga a Neon. Si la base ya tenía información, los conteos globales deben complementarse con la revisión de claves y contadores de la carga.

## 8. Pruebas ejecutadas y límites de la verificación

La suite ejecutada durante la implementación obtuvo **91 pruebas aprobadas, 5 omitidas y 1 advertencia en 42,61 segundos**. Después del ajuste final de las columnas explícitas de COPY se repitieron las pruebas SQL sin TCP: **15 aprobadas, 1 omitida y 2 deseleccionadas**. También se verificaron la compilación Python y el formato de los cambios con `git diff --check`.

Las pruebas cubren configuración deshabilitada, columnas, tipos, precisión, separación de tablas, COPY, duplicados, reenvíos, rollback simulado, errores de conexión, compatibilidad con MongoDB y ACK mediante sockets reales. La persistencia SQL se verificó con dobles de prueba; esto no sustituye una ejecución contra el motor PostgreSQL real.

```powershell
python -m pytest -q --disable-warnings
```

La prueba opcional de conexión real usa `NEON_TEST_DATABASE_URL` y solo realiza consultas de lectura:

```powershell
python -m pytest tests/test_sql_repositorio.py -k neon_real -q
```

No se contó con el CSV real ni credenciales Neon durante la implementación. No se ha medido la duración de la persistencia remota. Los 42,61 segundos corresponden a la suite de pruebas, no a una carga del dataset real.

El cliente espera una respuesta durante 60 segundos. Los tiempos de MongoDB, validación SQL, PostgreSQL y aceptación por la GUI se suman, por lo que aún debe medirse una carga completa en una base de pruebas. Los timeouts SQL limitan operaciones individuales, no todo el proceso. Si se supera el plazo, será necesario ajustar los tiempos de espera de forma coordinada a partir de esa medición.

El bloqueo asesor coordina únicamente escritores que usan el mismo mecanismo. Otras aplicaciones que escriban sobre las tablas requieren coordinación adicional para mantener las mismas comprobaciones de consistencia.

## 9. Ubicación de la implementación

[Rama publicada en GitHub](https://github.com/cescalantea/bigdata_evaluacion2/tree/jose/conexion_neon_postgresql)

[Guía operativa de Neon](neon_postgresql.md)
