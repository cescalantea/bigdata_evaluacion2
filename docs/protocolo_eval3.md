# Protocolo TCP de la Evaluacion 3

El servidor usa dos puertos y dos hilos de escucha independientes:

```text
CSV      -> 127.0.0.1:12345 -> recibe JSON masivo + <EOF>
Sensores -> 127.0.0.1:12346 -> recibe JSON Lines type=SENSOR
```

Una conexion de sensores no compite con la conexion del CSV. Cada conexion se
atiende en su propio hilo, por lo que el CSV puede seguir transfiriendose sin
detener la recepcion de lecturas ni bloquear la interfaz grafica.

## Puerto CSV

Se mantiene el contrato de la Evaluacion 2 para no romper el cliente existente:

```text
[{"columna": "valor"}, {"columna": "valor"}]<EOF>
```

El servidor valida la marca final, convierte el arreglo a Pandas, guarda el CSV
en MongoDB por lotes y envia el ACK JSON habitual.

## Puerto de sensores

El grupo cliente debe abrir una conexion persistente a `127.0.0.1:12346` y
enviar un objeto JSON por linea, terminado en `\n`:

```json
{"type":"SENSOR","timestamp":"2026-11-14T14:32:10","temperature":23.8,"humidity":56.4,"gas":412}
```

El servidor acepta tambien las claves cortas `temp` y `hum`. La respuesta por
cada lectura es:

```json
{"ok":true,"type":"SENSOR_ACK","persisted":true,"sensores":1}
```

Al cerrar el canal, el servidor envia un resumen:

```json
{"ok":true,"type":"SENSOR_STREAM_ACK","sensores":10}
```

## Acuerdo entre grupos

Antes de integrar Arduino, ambos equipos deben confirmar:

- puerto de sensores: `12346`;
- delimitador de mensajes: una linea JSON terminada en `\n`;
- campos: `timestamp`, `temperature`, `humidity`, `gas`;
- unidades: grados Celsius, porcentaje de humedad y ppm para gas;
- ACK por lectura y resumen final;
- comportamiento ante reconexion o error.

