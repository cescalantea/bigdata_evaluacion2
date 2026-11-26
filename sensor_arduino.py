"""
SENSOR ARDUINO - lectura por puerto serie (pyserial) y grabación con tasa de muestreo.

Sin GUI ni red: así se puede probar sin Arduino (con un puerto falso).
El Arduino manda líneas "temperatura,humedad,gas" (ver scrip_arduino_sensor_v1.txt).

Requisitos: pip install pyserial
"""

import csv
import json
import math
import os
import threading
import time
from datetime import datetime

BAUDIOS = 9600                 # debe coincidir con Serial.begin() del Arduino
# Simulador (simulador_arduino.py): pyserial abre "socket://..." igual que un COM, sin drivers.
PUERTO_SIMULADOR = int(os.environ.get("SIMULADOR_PUERTO", "5577"))
URL_SIMULADOR = f"socket://127.0.0.1:{PUERTO_SIMULADOR}"
MAX_EDAD_LECTURA = 6.0         # s: una lectura más vieja no se graba (sensor caído o desconectado)

# Tres alternativas de captura: nombre en pantalla -> segundos entre muestras.
# El DHT11 entrega 1 dato por segundo como máximo (1 Hz), así que 1 s es la tasa más rápida posible.
TASAS = {"1 dato por segundo": 1.0, "1 dato cada 2 segundos": 2.0, "1 dato cada 3 segundos": 3.0}

# Rangos admitidos: lo que llega por serie no es de confianza (ruido, línea corrupta).
RANGOS = {"temperatura": (-40.0, 80.0), "humedad": (0.0, 100.0), "gas": (0.0, 1023.0)}
CAMPOS = tuple(RANGOS)

# --- Semáforo de niveles (solo para mostrar; no filtra ni modifica los datos que se envían) ---
# Límites operativos y de medición del DHT11 según su hoja de datos. Fuera de ellos la lectura no
# es confiable: rojo. Escala de cada barra del ecualizador = este rango.
LIMITES_SENSOR = {"temperatura": (0.0, 50.0), "humedad": (20.0, 90.0), "gas": (0.0, 1023.0)}
MARGEN_ALERTA = 0.10           # amarillo: el 10 % del rango más cercano a cada límite del DHT11
# MQ-2: lectura analógica relativa (no son ppm). Umbrales de referencia, SIN calibrar con la placa.
GAS_PRECAUCION = 300           # desde aquí, amarillo
GAS_PELIGRO = 600              # desde aquí, rojo
VERDE, AMARILLO, ROJO = "verde", "amarillo", "rojo"


# Propósito: nivel del semáforo de un valor. Temperatura y humedad usan los límites del DHT11
# (rojo fuera del rango, amarillo cerca de los bordes); el gas usa los umbrales de referencia.
def nivel(campo, valor):
    if campo == "gas":
        return ROJO if valor >= GAS_PELIGRO else AMARILLO if valor >= GAS_PRECAUCION else VERDE
    minimo, maximo = LIMITES_SENSOR[campo]
    if not minimo <= valor <= maximo:
        return ROJO
    margen = (maximo - minimo) * MARGEN_ALERTA
    return AMARILLO if valor < minimo + margen or valor > maximo - margen else VERDE


# Propósito: rango que muestra la barra del ecualizador. Para temperatura y humedad son los límites del
# DHT11 más un 10 % a cada lado, para que la zona roja de "fuera de rango" tenga espacio visible.
# El gas usa su escala completa (0 a 1023).
def escala(campo):
    minimo, maximo = LIMITES_SENSOR[campo]
    if campo == "gas":
        return minimo, maximo
    extra = (maximo - minimo) * MARGEN_ALERTA
    return minimo - extra, maximo + extra


class ErrorSensor(Exception):
    """No se pudo usar el puerto serie."""


# Propósito: convertir "24.5,60.0,0" en un dict. Devuelve None si la línea no sirve
# (ERROR_LECTURA, texto cortado al conectar, valores fuera de rango o NaN).
def parsear_linea(texto):
    partes = texto.strip().split(",")
    if len(partes) != len(CAMPOS):
        return None
    try:
        valores = [float(p) for p in partes]
    except ValueError:
        return None
    for campo, valor in zip(CAMPOS, valores):
        minimo, maximo = RANGOS[campo]
        if not math.isfinite(valor) or not minimo <= valor <= maximo:
            return None
    return dict(zip(CAMPOS, valores))


# Propósito: nombres de los puertos serie disponibles (COM3, /dev/ttyACM0...).
def listar_puertos():
    try:
        from serial.tools import list_ports
    except ImportError as error:
        raise ErrorSensor("Falta pyserial: ejecute  pip install pyserial") from error
    return sorted(p.device for p in list_ports.comports())


# Propósito: abrir el puerto serie (COM3, /dev/ttyACM0) o la URL del simulador (socket://...).
# timeout corto para poder revisar la orden de parar.
def abrir_serial(puerto, baudios=BAUDIOS):
    try:
        import serial
    except ImportError as error:
        raise ErrorSensor("Falta pyserial: ejecute  pip install pyserial") from error
    try:
        return serial.serial_for_url(puerto, baudrate=baudios, timeout=0.5)
    except (serial.SerialException, ValueError, OSError) as error:
        pista = " ¿Está encendido el simulador?" if puerto.startswith("socket://") else ""
        raise ErrorSensor(f"No se pudo abrir {puerto}: {error}.{pista}") from error


class LectorSerial:
    """
    Lee el puerto en un hilo y guarda siempre la última lectura válida.
    'abrir' se inyecta (por defecto abrir_serial) para poder probar con un puerto falso.
    """

    def __init__(self, puerto, al_lectura=lambda lectura: None, al_error=lambda texto: None,
                 abrir=abrir_serial):
        self.puerto = puerto
        self._al_lectura, self._al_error = al_lectura, al_error
        self._abrir = abrir
        self._parar = threading.Event()
        self._hilo = None
        self._serie = None
        self._candado = threading.Lock()
        self._ultima = None            # (lectura, instante monotónico)

    # Propósito: abrir el puerto (lanza ErrorSensor) y arrancar el hilo lector.
    def iniciar(self):
        self._serie = self._abrir(self.puerto)
        self._hilo = threading.Thread(target=self._bucle, daemon=True)
        self._hilo.start()

    # Propósito: detener el hilo y cerrar el puerto.
    def detener(self):
        self._parar.set()
        if self._hilo is not None:
            self._hilo.join(timeout=2)
        try:
            self._serie.close()
        except Exception:
            pass

    # Propósito: la última lectura si es reciente; None si no hay o ya caducó.
    def ultima(self):
        with self._candado:
            if self._ultima is None:
                return None
            lectura, instante = self._ultima
        return lectura if time.monotonic() - instante <= MAX_EDAD_LECTURA else None

    # Propósito: (hilo) leer líneas hasta que se pida parar o falle el cable.
    # readline() devuelve lo que alcanzó a llegar si vence el timeout a mitad de línea:
    # se acumula en 'pendiente' hasta ver el salto de línea, para no perder esa lectura.
    def _bucle(self):
        pendiente = b""
        while not self._parar.is_set():
            try:
                pendiente += self._serie.readline()
            except Exception as error:   # cable desconectado: SerialException u OSError
                if not self._parar.is_set():
                    self._al_error(f"Se perdió la conexión con {self.puerto}: {error}")
                return
            if not pendiente.endswith(b"\n"):
                if len(pendiente) > 256:  # basura sin salto de línea: no crecer sin límite
                    pendiente = b""
                continue
            linea, pendiente = pendiente.decode("ascii", errors="ignore"), b""
            lectura = parsear_linea(linea)
            if lectura is not None:
                with self._candado:
                    self._ultima = (lectura, time.monotonic())
                self._al_lectura(lectura)


# Propósito: guardar las filas capturadas como CSV. Las columnas son exactamente las que se envían
# al servidor: fecha_hora, temperatura, humedad, gas.
def exportar_csv(ruta, registros):
    with open(ruta, "w", encoding="utf-8", newline="") as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=("fecha_hora",) + CAMPOS)
        escritor.writeheader()
        escritor.writerows(registros)


# Propósito: guardar las filas capturadas como JSON: una lista de objetos, igual que viaja por el socket.
def exportar_json(ruta, registros):
    with open(ruta, "w", encoding="utf-8") as archivo:
        json.dump(registros, archivo, ensure_ascii=False, indent=2, allow_nan=False)


class Grabadora:
    """
    Acumula filas (fecha_hora, temperatura, humedad, gas) mientras está grabando.
    El reloj de muestreo lo pone la GUI (after); aquí solo se decide qué fila guardar.
    Guarda además la fecha y hora de inicio y de cierre, para la trazabilidad de la transmisión.
    """

    def __init__(self, ahora=None):
        self.registros = []
        self.inicio = ahora or datetime.now()     # momento en que se pulsó "Iniciar transmisión"
        self.cierre = None                        # None mientras la transmisión sigue en curso

    # Propósito: marcar el cierre de la transmisión. Solo cuenta la primera llamada.
    def cerrar(self, ahora=None):
        if self.cierre is None:
            self.cierre = ahora or datetime.now()
        return self.cierre

    # Propósito: duración en segundos (hasta ahora si todavía no se cerró).
    def duracion(self, ahora=None):
        return ((self.cierre or ahora or datetime.now()) - self.inicio).total_seconds()

    # Propósito: guardar una fila con la lectura dada. Devuelve False si no hay lectura vigente.
    def muestrear(self, lectura, ahora=None):
        if lectura is None:
            return False
        marca = (ahora or datetime.now()).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        self.registros.append({"fecha_hora": marca, **lectura})
        return True
