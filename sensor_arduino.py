"""
SENSOR ARDUINO - lectura por puerto serie (pyserial) y grabación con tasa de muestreo.

Sin GUI ni red: así se puede probar sin Arduino (con un puerto falso).
El Arduino manda líneas "temperatura,humedad,gas" (ver scrip_arduino_sensor_v1.txt).

Requisitos: pip install pyserial
"""

import math
import threading
import time
from datetime import datetime

BAUDIOS = 9600                 # debe coincidir con Serial.begin() del Arduino
MAX_EDAD_LECTURA = 6.0         # s: una lectura más vieja no se graba (sensor caído o desconectado)

# Tres alternativas de captura: nombre en pantalla -> segundos entre muestras.
# El Arduino debe enviar al menos a 5 líneas/s (ver scrip_arduino_sensor_v2.txt).
TASAS = {"1 dato por segundo": 1.0, "2 datos por segundo": 0.5, "5 datos por segundo": 0.2}

# Rangos admitidos: lo que llega por serie no es de confianza (ruido, línea corrupta).
RANGOS = {"temperatura": (-40.0, 80.0), "humedad": (0.0, 100.0), "gas": (0.0, 1023.0)}
CAMPOS = tuple(RANGOS)


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


# Propósito: abrir el puerto serie. timeout corto para poder revisar la orden de parar.
def abrir_serial(puerto, baudios=BAUDIOS):
    try:
        import serial
    except ImportError as error:
        raise ErrorSensor("Falta pyserial: ejecute  pip install pyserial") from error
    try:
        return serial.Serial(puerto, baudios, timeout=0.5)
    except (serial.SerialException, ValueError, OSError) as error:
        raise ErrorSensor(f"No se pudo abrir {puerto}: {error}") from error


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
    def _bucle(self):
        while not self._parar.is_set():
            try:
                linea = self._serie.readline().decode("ascii", errors="ignore")
            except Exception as error:   # cable desconectado: SerialException u OSError
                if not self._parar.is_set():
                    self._al_error(f"Se perdió la conexión con {self.puerto}: {error}")
                return
            lectura = parsear_linea(linea) if linea else None
            if lectura is not None:
                with self._candado:
                    self._ultima = (lectura, time.monotonic())
                self._al_lectura(lectura)


class Grabadora:
    """
    Acumula filas (fecha_hora, temperatura, humedad, gas) mientras está grabando.
    El reloj de muestreo lo pone la GUI (after); aquí solo se decide qué fila guardar.
    """

    def __init__(self):
        self.registros = []

    # Propósito: guardar una fila con la lectura dada. Devuelve False si no hay lectura vigente.
    def muestrear(self, lectura, ahora=None):
        if lectura is None:
            return False
        marca = (ahora or datetime.now()).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        self.registros.append({"fecha_hora": marca, **lectura})
        return True
