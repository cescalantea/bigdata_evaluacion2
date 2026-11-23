# Flujo lógico de la aplicación cliente (CSV + Arduino)

Rama `rama-arduino`. Este documento describe cómo viaja un dato desde el sensor
hasta la tabla del servidor, y por qué se tomaron las decisiones principales.

## 1. Visión general

La aplicación cliente tiene **dos pestañas** que comparten un mismo destino: el servidor TCP.
Hay **dos conexiones distintas** y no deben confundirse:

| Conexión | Entre | Tecnología | Quién la maneja |
|---|---|---|---|
| Serie (USB) | Arduino → aplicación | `pyserial`, 9600 baudios | `LectorSerial` (`sensor_arduino.py`) |
| TCP | Aplicación → servidor | `socket`, JSON + `<EOF>` | `EmisorTCP` (`cliente.py`) |

```mermaid
flowchart TD
    A["Arduino Uno<br/>DHT11 + MQ-2"] -->|"línea CSV por USB<br/>cada 1 s"| B["LectorSerial<br/>hilo lector"]
    B -->|"parsear_linea valida rango"| C{"¿Línea válida?"}
    C -->|No| D["Se descarta<br/>ERROR_LECTURA, ruido"]
    C -->|Sí| E["Última lectura<br/>vigente 6 s"]
    E -->|"cola de tareas"| F["GUI: 3 gráficos de línea<br/>temperatura, humedad, gas"]
    E -->|"cada 1, 2 o 3 s"| G["Grabadora<br/>filas en memoria"]
    G -->|"Detener transmisión"| H["EmisorTCP<br/>JSON por lotes + EOF"]
    I["Pestaña CSV<br/>Dataset + selección de columnas"] -->|"Enviar datos"| H
    H -->|"TCP"| J["Servidor<br/>valida, muestra y responde ACK"]
    J -->|"ACK JSON"| K["GUI: confirma filas y demora"]
```

## 2. Formato de los datos

El Arduino envía una línea por lectura. La aplicación la convierte en una fila:

```text
24.5,60.0,312        →   fecha_hora            temperatura  humedad  gas
                         2026-11-16 17:02:31.250   24.5        60.0     312.0
```

`parsear_linea` rechaza cualquier cosa fuera de rango, porque lo que llega por el cable
no es de confianza (ruido, línea cortada al conectar, `NaN`):

| Campo | Rango aceptado |
|---|---|
| temperatura | −40 a 80 °C |
| humedad | 0 a 100 % |
| gas | 0 a 1023 (valor relativo del MQ-2, no son ppm) |

## 3. Secuencia completa de una transmisión

```mermaid
sequenceDiagram
    autonumber
    actor U as Usuario
    participant G as GUI (hilo principal)
    participant L as LectorSerial (hilo)
    participant R as Grabadora
    participant E as EmisorTCP (hilo)
    participant S as Servidor

    U->>G: Conectar Arduino
    G->>L: iniciar() abre el puerto
    loop cada 1 s
        L->>L: readline() + parsear_linea()
        L->>G: lectura válida (por la cola)
        G->>G: actualiza los 3 gráficos
    end

    U->>G: Iniciar transmisión
    G->>G: valida IP y puerto, no hay envío en curso
    G->>R: nueva Grabadora
    loop según la tasa elegida
        G->>L: ultima()
        L-->>G: lectura vigente o None
        G->>R: muestrear(lectura)
    end

    U->>G: Detener transmisión
    G->>G: cancela el temporizador
    alt sin filas capturadas
        G-->>U: aviso, no se envía nada
    else con filas
        G->>E: DataFrame de las filas, lotes de 1.000
        E->>S: conectar
        E->>S: "[" + lotes JSON + "]" + EOF
        E->>S: shutdown de escritura
        S->>S: decodifica, valida y muestra la tabla
        S-->>E: ACK JSON con ok y filas
        E-->>G: resultado (por la cola)
        G-->>U: "El servidor confirmó N filas" y demora
    end
```

## 4. Estados de la pestaña Arduino

```mermaid
stateDiagram-v2
    [*] --> Desconectado
    Desconectado --> Conectado: Conectar Arduino
    Conectado --> Desconectado: Desconectar o cable suelto
    Conectado --> Transmitiendo: Iniciar transmisión
    Transmitiendo --> Enviando: Detener transmisión con filas
    Transmitiendo --> Conectado: Detener transmisión sin filas
    Transmitiendo --> Desconectado: Desconectar o cable suelto, se descarta lo capturado
    Enviando --> Conectado: ACK, error o cancelación
```

Qué ocurre en cada estado:

- **Desconectado:** solo se ve la fila de conexión. Los gráficos aparecen al conectar.
- **Conectado:** los gráficos se actualizan con cada línea válida. Todavía no se guarda nada.
- **Transmitiendo:** además de graficar, se guarda una fila por tic de la tasa elegida.
  Mientras tanto, el envío del CSV queda deshabilitado para no mezclar ambos flujos.
- **Enviando:** hilo aparte; la ventana sigue respondiendo y se puede **cancelar**.

## 5. Las tres frecuencias de captura

El **DHT11 entrega un dato nuevo por segundo como máximo (1 Hz)**. Por eso la frecuencia más
rápida posible es 1 dato por segundo, y las otras dos son más lentas. El Arduino (script v3)
envía una línea por segundo, y la aplicación decide cada cuánto **guarda** una fila.

```mermaid
flowchart TD
    T["Tic del temporizador<br/>1 s, 2 s o 3 s"] --> Q{"¿Hay lectura<br/>de hace menos de 6 s?"}
    Q -->|Sí| W["Guarda la fila<br/>fecha_hora + 3 valores"]
    Q -->|No| X["Omite el tic<br/>sensor caído o sin datos"]
    W --> N["Programa el siguiente tic"]
    X --> N
```

| Opción | Intervalo | Filas por minuto |
|---|---|---|
| 1 dato por segundo | 1 s | 60 |
| 1 dato cada 2 segundos | 2 s | 30 |
| 1 dato cada 3 segundos | 3 s | 20 |

> **Historial:** las versiones anteriores ofrecían 2 y 5 datos por segundo, que el DHT11
> no puede entregar: se repetían valores. Se retiraron y el script v3 envía a 1 Hz.

## 5.1 Trazabilidad de la transmisión

Al pulsar **Iniciar transmisión** la `Grabadora` registra la fecha y hora de inicio, y al
pulsar **Detener transmisión** (o al cortarse el cable) registra el cierre. El panel
"Trazabilidad de la transmisión" de la pestaña Arduino lo muestra:

```text
Inicio: 23/11/2026 15:28:35   ·   Cierre: 23/11/2026 15:31:02
Duración: 2 min 27 s   ·   147 filas   ·   Frecuencia: 1 dato por segundo
```

```mermaid
flowchart TD
    A["Iniciar transmisión"] --> B["Se registra el inicio"]
    B --> C["Panel: Cierre en curso,<br/>se actualiza en cada tic"]
    C --> D{"¿Cómo termina?"}
    D -->|"Detener transmisión"| E["Se registra el cierre<br/>y se envía al servidor"]
    D -->|"Se corta el cable"| F["Se registra el cierre<br/>como Interrumpida: no se envía"]
```

Además, **cada fila** lleva su propia `fecha_hora` (con milisegundos), así que el servidor
recibe la trazabilidad fila por fila y no solo el inicio y el cierre.

## 5.2 Exportar lo capturado (CSV y JSON)

En el panel de trazabilidad, los botones **CSV…** y **JSON…** guardan las filas de la última
transmisión, es decir, las mismas que se envían al servidor. Se habilitan al detener la
transmisión (o si se cortó el cable) y se deshabilitan mientras se transmite.

Ambos formatos llevan **solo cuatro campos**: `fecha_hora`, `temperatura`, `humedad`, `gas`.

```text
CSV                                          JSON (igual que viaja por el socket)
fecha_hora,temperatura,humedad,gas           [ { "fecha_hora": "2026-11-23 15:00:00.123",
2026-11-23 15:00:00.123,23.0,55.0,134.0        "temperatura": 23.0, "humedad": 55.0, "gas": 134.0 } ]
```

El JSON exportado es idéntico a lo que recibe el servidor (hay una prueba que lo comprueba).
Sirve para comparar cómo cambia el tipo de dato: en CSV todo es texto y el lector decide el
tipo; en JSON los números llegan ya como números.

## 6. Protocolo hacia el servidor (sin cambios)

El servidor es del Grupo 4 y no se modificó. La transmisión Arduino reutiliza el mismo
contrato que el envío de CSV:

```mermaid
flowchart TD
    A["Cliente abre conexión TCP"] --> B["Envía '[' y los lotes JSON<br/>separados por coma"]
    B --> C["Envía ']' + marca EOF"]
    C --> D["shutdown de escritura:<br/>no enviaré más"]
    D --> E["Servidor lee hasta que el cliente cierra"]
    E --> F{"¿JSON válido,<br/>lista de objetos?"}
    F -->|Sí| G["Muestra la tabla y responde<br/>ok=true, filas=N"]
    F -->|No| H["Responde ok=false<br/>con el detalle del error"]
    G --> I["Cliente compara filas enviadas<br/>con las confirmadas"]
    H --> I
```

### ¿Por qué se envía todo al detener y no fila a fila?

El nombre "Iniciar transmisión" sugiere un envío en vivo, pero el envío ocurre al detener.
Razones, todas del lado del servidor actual:

1. **Reemplaza su tabla en cada conexión.** Enviar por tandas dejaría solo la última tanda.
2. **Corta cada conexión a los 600 s** (`SERVIDOR_TIEMPO_TOTAL`). Una sola conexión larga
   fallaría en grabaciones largas.
3. **Solo muestra los datos al recibir `<EOF>`.** El envío en vivo no se vería antes del final.

Enviar al detener evita las tres sin tocar el servidor. El costo: si la aplicación se cierra
mientras se transmite, lo capturado se pierde. Para verlo en vivo en el servidor habría que
acordar con el Grupo 4 que **acumule** filas en vez de reemplazarlas.

## 7. Hilos y comunicación con la ventana

Tkinter no es seguro entre hilos: **solo el hilo principal toca los widgets**. Los hilos
dejan tareas en una cola y la ventana las ejecuta cada 100 ms (`_revisar_cola`).

```mermaid
flowchart TD
    subgraph P["Hilo principal: ventana"]
        A["Widgets y gráficos"]
        B["Temporizador de muestreo<br/>after()"]
        C["_revisar_cola cada 100 ms"]
    end
    subgraph H1["Hilo lector serie"]
        D["readline y parseo"]
    end
    subgraph H2["Hilo de envío TCP"]
        E["EmisorTCP.enviar"]
    end
    D -->|"cola"| C
    E -->|"cola: progreso y resultado"| C
    C --> A
    B -->|"ultima()"| D
```

## 8. Errores contemplados

| Situación | Respuesta de la aplicación |
|---|---|
| Falta `pyserial` | La tarjeta avisa "Falta pyserial: ejecute pip install pyserial" |
| No hay puertos | "No se detectó ningún Arduino." |
| Puerto ocupado o inexistente | Mensaje claro; no queda conectado |
| Se desconecta el cable | Se desconecta solo y descarta lo capturado |
| Línea corrupta o `ERROR_LECTURA` | Se ignora; no entra a gráficos ni a la tabla |
| IP o puerto inválidos | Se avisa **antes** de empezar a capturar |
| Detener sin filas | Aviso; no se abre conexión |
| Servidor apagado | "No hay un servidor escuchando…"; se puede reintentar |
| Cancelar el envío | Se corta y la pantalla queda lista de nuevo |

## 9. Archivos y pruebas

| Archivo | Rol |
|---|---|
| `sensor_arduino.py` | Lógica del sensor sin GUI: parseo, lector serie, grabadora, tasas |
| `cliente.py` | Ventana con pestañas, gráficos (`GraficoLinea`) y `EmisorTCP` |
| `scrip_arduino_sensor_v3.txt` | Código para el Arduino: 1 línea por segundo (límite del DHT11) |
| `tests/test_sensor.py` | 22 pruebas con un puerto serie simulado, sin hardware |

Seguridad aplicada: validación de rango de todo lo que llega por el cable, límite de
antigüedad de lectura, y sin secretos ni datos sensibles en pantalla ni logs.

**Pendiente de validar:** prueba con el Arduino real y el MQ-2 conectado, y ejecución
en Linux (el repositorio se valida solo en Windows por decisión del equipo).
