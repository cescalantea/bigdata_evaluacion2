"""Pruebas del simulador de Arduino y de su conexión con la aplicación. Ejecutar:  python -m pytest -q"""

import csv
import json
import socket
import threading
import time
import tkinter as tk
from datetime import datetime

import pytest

import sensor_arduino
import servidor_prueba
from cliente import VentanaCliente
from sensor_arduino import LectorSerial, abrir_serial, parsear_linea
from simulador_arduino import (CAMPOS_EXPORTACION, EmisorSerie, GeneradorSensor, armar_registro,
                               exportar_csv, exportar_json)


# Propósito: esperar (sin dormir de más) a que se cumpla una condición.
def esperar(condicion, tiempo=3.0):
    limite = time.monotonic() + tiempo
    while not condicion() and time.monotonic() < limite:
        time.sleep(0.01)
    return condicion()


# --- Generador ---------------------------------------------------------------------

def test_misma_semilla_misma_secuencia():
    a, b = GeneradorSensor(semilla=7), GeneradorSensor(semilla=7)
    assert [a.siguiente() for _ in range(300)] == [b.siguiente() for _ in range(300)]


def test_sin_semilla_guarda_la_elegida_para_repetir_la_corrida():
    a = GeneradorSensor()
    b = GeneradorSensor(semilla=a.semilla)
    assert [a.siguiente() for _ in range(50)] == [b.siguiente() for _ in range(50)]


def test_toda_linea_generada_la_acepta_la_aplicacion():
    g = GeneradorSensor(semilla=1, prob_error=0.05)
    errores = 0
    for _ in range(3000):
        lectura = g.siguiente()
        linea = GeneradorSensor.linea(lectura)
        if lectura is None:
            errores += 1
            assert linea == "ERROR_LECTURA" and parsear_linea(linea) is None
        else:
            assert parsear_linea(linea) == {k: float(v) for k, v in lectura.items()}
    assert 50 < errores < 300          # ~5 % de 3.000: hay fallos, pero no son la norma


def test_dht11_entrega_enteros_y_respeta_su_rango():
    g = GeneradorSensor(semilla=3, prob_error=0)
    lecturas = [g.siguiente() for _ in range(2000)]
    assert all(l["temperatura"] == int(l["temperatura"]) and 0 <= l["temperatura"] <= 50 for l in lecturas)
    assert all(l["humedad"] == int(l["humedad"]) and 20 <= l["humedad"] <= 90 for l in lecturas)
    assert all(0 <= l["gas"] <= 1023 for l in lecturas)


def test_modo_decimal_entrega_un_decimal():
    g = GeneradorSensor(semilla=3, dht_entero=False, prob_error=0)
    temperaturas = [g.siguiente()["temperatura"] for _ in range(50)]
    assert any(t != int(t) for t in temperaturas)
    assert all(t == round(t, 1) for t in temperaturas)


def test_precalentamiento_del_mq2_baja_con_el_tiempo():
    g = GeneradorSensor(semilla=5, prob_error=0)
    gases = [g.siguiente()["gas"] for _ in range(120)]
    assert gases[0] > 350 and max(gases[90:]) < 200   # arranca alto y se estabiliza


def test_evento_de_gas_sube_y_luego_decae():
    g = GeneradorSensor(semilla=5, prob_error=0)
    for _ in range(100):
        g.siguiente()
    base = g.siguiente()["gas"]
    g.evento_gas()
    pico = g.siguiente()["gas"]
    for _ in range(120):
        final = g.siguiente()["gas"]
    assert pico > base + 300 and final < base + 40


# --- Exportación -------------------------------------------------------------------------

REGISTROS = [armar_registro(datetime(2026, 11, 23, 15, 0, 0, 123000),
                            {"temperatura": 23.0, "humedad": 55.0, "gas": 134}),
             armar_registro(datetime(2026, 11, 23, 15, 0, 1), None)]


def test_csv_tiene_columnas_origen_y_vacios_en_los_fallos(tmp_path):
    ruta = tmp_path / "datos.csv"
    exportar_csv(ruta, REGISTROS)
    with open(ruta, encoding="utf-8", newline="") as archivo:
        filas = list(csv.DictReader(archivo))
    assert tuple(filas[0]) == CAMPOS_EXPORTACION
    assert filas[0] == {"fecha_hora": "2026-11-23 15:00:00.123", "temperatura": "23.0", "humedad": "55.0",
                        "gas": "134", "estado": "OK", "origen": "simulado"}
    assert filas[1]["temperatura"] == "" and filas[1]["estado"] == "ERROR_LECTURA"


def test_json_declara_origen_semilla_y_resumen(tmp_path):
    ruta = tmp_path / "datos.json"
    exportar_json(ruta, REGISTROS, {"semilla": 42, "frecuencia_s": 1.0})
    doc = json.loads(ruta.read_text(encoding="utf-8"))
    assert doc["origen"] == "simulado" and doc["semilla"] == 42 and "sintéticos" in doc["aviso"]
    assert doc["resumen"] == {"registros": 2, "errores_lectura": 1,
                              "inicio": "2026-11-23 15:00:00.123", "fin": "2026-11-23 15:00:01.000"}
    assert doc["registros"][1]["temperatura"] is None


# --- Conexión real por socket:// -------------------------------------------------------------

@pytest.fixture
def emisor():
    e = EmisorSerie(0)                 # puerto 0: el sistema elige uno libre (no choca con otras pruebas)
    e.iniciar()
    yield e
    e.detener()


def test_emisor_sin_aplicacion_no_envia(emisor):
    assert emisor.enviar("23.0,55.0,134") is False and not emisor.hay_cliente


def test_la_aplicacion_lee_las_lineas_del_simulador_por_socket(emisor):
    recibidas = []
    lector = LectorSerial(f"socket://127.0.0.1:{emisor.puerto}", al_lectura=recibidas.append)
    lector.iniciar()                   # abrir_serial real de pyserial (serial_for_url)
    assert esperar(lambda: emisor.hay_cliente)
    g = GeneradorSensor(semilla=2, prob_error=0)
    enviados = []
    for _ in range(5):
        lectura = g.siguiente()
        assert emisor.enviar(GeneradorSensor.linea(lectura))
        enviados.append({k: float(v) for k, v in lectura.items()})
    assert esperar(lambda: len(recibidas) == 5)
    assert recibidas == enviados       # lo que llega es exactamente lo que se envió
    lector.detener()


def test_lector_no_pierde_una_linea_partida(emisor):
    # Si el cable se detiene a mitad de línea, readline() devuelve un trozo: debe esperarse el resto.
    recibidas = []
    lector = LectorSerial(f"socket://127.0.0.1:{emisor.puerto}", al_lectura=recibidas.append)
    lector.iniciar()
    assert esperar(lambda: emisor.hay_cliente)
    with emisor._candado:
        emisor._cliente.sendall(b"24.0,5")
    time.sleep(0.8)                    # más que el timeout de lectura (0,5 s)
    with emisor._candado:
        emisor._cliente.sendall(b"6.0,140\r\n")
    assert esperar(lambda: recibidas == [{"temperatura": 24.0, "humedad": 56.0, "gas": 140.0}])
    lector.detener()


def test_apagar_el_simulador_se_informa_como_cable_perdido(emisor):
    errores = []
    lector = LectorSerial(f"socket://127.0.0.1:{emisor.puerto}", al_error=errores.append)
    lector.iniciar()
    assert esperar(lambda: emisor.hay_cliente)
    emisor.detener()
    assert esperar(lambda: errores) and "Se perdió la conexión" in errores[0]
    lector.detener()


def test_conectar_con_el_simulador_apagado_sugiere_encenderlo():
    libre = socket.create_server(("127.0.0.1", 0))
    puerto = libre.getsockname()[1]
    libre.close()                      # nadie escucha en ese puerto
    with pytest.raises(sensor_arduino.ErrorSensor, match="simulador"):
        abrir_serial(f"socket://127.0.0.1:{puerto}")


# --- De punta a punta: simulador -> ventana -> servidor -------------------------------------

def test_simulador_a_aplicacion_a_servidor(emisor):
    try:
        v = VentanaCliente()
    except tk.TclError:
        pytest.skip("No hay pantalla disponible para Tkinter")
    v.withdraw()
    escucha = socket.create_server(("127.0.0.1", 0))
    resultado = {}

    def servir():
        conexion, _ = escucha.accept()
        with conexion:
            resultado.update(servidor_prueba.atender(conexion))
    threading.Thread(target=servir, daemon=True).start()
    try:
        assert sensor_arduino.URL_SIMULADOR in v.combo_puerto.cget("values")   # aparece en la lista
        v.combo_puerto.set(f"socket://127.0.0.1:{emisor.puerto}")
        v.var_puerto.set(str(escucha.getsockname()[1]))
        v.conectar_arduino()
        assert v.lector is not None and esperar(lambda: emisor.hay_cliente)

        g = GeneradorSensor(semilla=9, prob_error=0)
        emitidas = []
        for i in range(4):             # el simulador envía y la aplicación toma una muestra de cada lectura
            lectura = g.siguiente()
            esperada = {k: float(x) for k, x in lectura.items()}
            assert emisor.enviar(GeneradorSensor.linea(lectura))
            emitidas.append(esperada)
            assert esperar(lambda: v.lector.ultima() == esperada)
            if i == 0:
                v.iniciar_grabacion()  # toma la primera muestra de inmediato
            else:
                v._parar_muestreo()
                v._tomar_muestra()
        v.detener_grabacion()
        assert esperar(lambda: (v.update() or v.emisor is None), tiempo=5)
    finally:
        v.desconectar_arduino()
        escucha.close()
        v.destroy()
    assert resultado.get("ok") and resultado["filas"] == 4
    recibidas = [{k: f[k] for k in ("temperatura", "humedad", "gas")} for f in resultado["registros"]]
    assert recibidas == emitidas       # el servidor recibe exactamente lo que emitió el simulador
