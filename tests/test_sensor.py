"""Pruebas del sensor Arduino (sin hardware: puerto serie falso). Ejecutar:  python -m pytest -q"""

import csv
import json
import socket
import threading
import time
import tkinter as tk

import pytest

import sensor_arduino
import servidor_prueba
from cliente import VentanaCliente
from datetime import datetime, timedelta

from sensor_arduino import Grabadora, LectorSerial, parsear_linea


# Propósito: puerto serie falso. readline() entrega las líneas dadas y luego "no llega nada".
class SerieFalso:
    def __init__(self, lineas):
        self.lineas = list(lineas)
        self.cerrado = False

    def readline(self):
        if self.lineas:
            return self.lineas.pop(0)
        time.sleep(0.01)
        return b""

    def close(self):
        self.cerrado = True


# --- Interpretar líneas -------------------------------------------------------------

def test_parsear_linea_valida():
    assert parsear_linea("24.5,60.0,0\r\n") == {"temperatura": 24.5, "humedad": 60.0, "gas": 0.0}


@pytest.mark.parametrize("linea", [
    "ERROR_LECTURA", "", "1,2", "1,2,3,4", "a,b,c", "nan,50,0", "inf,50,0",
    "200,50,0",       # temperatura fuera de rango
    "20,150,0",       # humedad fuera de rango
    "20,50,5000",     # gas fuera de rango
])
def test_parsear_linea_rechaza_basura(linea):
    assert parsear_linea(linea) is None


# --- Lector serie ---------------------------------------------------------------------

def test_lector_guarda_la_ultima_lectura_valida_e_ignora_basura():
    serie = SerieFalso([b"ERROR_LECTURA\r\n", b"20.0,50.0,1\r\n", b"\xff\xfe\r\n", b"21.0,51.0,2\r\n"])
    recibidas = []
    lector = LectorSerial("COM_FALSO", al_lectura=recibidas.append, abrir=lambda puerto: serie)
    lector.iniciar()
    limite = time.monotonic() + 2
    while len(recibidas) < 2 and time.monotonic() < limite:
        time.sleep(0.01)
    assert [r["temperatura"] for r in recibidas] == [20.0, 21.0]
    assert lector.ultima()["humedad"] == 51.0
    lector.detener()
    assert serie.cerrado


def test_lector_avisa_si_se_pierde_el_cable():
    class SerieRota(SerieFalso):
        def readline(self):
            raise OSError("cable desconectado")
    errores = []
    lector = LectorSerial("COM_FALSO", al_error=errores.append, abrir=lambda puerto: SerieRota([]))
    lector.iniciar()
    limite = time.monotonic() + 2
    while not errores and time.monotonic() < limite:
        time.sleep(0.01)
    assert errores and "cable desconectado" in errores[0]
    lector.detener()


def test_lectura_vieja_no_cuenta(monkeypatch):
    serie = SerieFalso([b"20.0,50.0,1\r\n"])
    lector = LectorSerial("COM_FALSO", abrir=lambda puerto: serie)
    lector.iniciar()
    limite = time.monotonic() + 2
    while lector.ultima() is None and time.monotonic() < limite:
        time.sleep(0.01)
    assert lector.ultima() is not None
    monkeypatch.setattr(sensor_arduino, "MAX_EDAD_LECTURA", -1)   # todo caduca al instante
    assert lector.ultima() is None
    lector.detener()


def test_abrir_serial_sin_puerto_da_error_claro():
    with pytest.raises(sensor_arduino.ErrorSensor):
        sensor_arduino.abrir_serial("COM_QUE_NO_EXISTE_999")


# --- Grabadora y tasas -----------------------------------------------------------------

def test_grabadora_guarda_filas_y_omite_si_no_hay_lectura():
    g = Grabadora()
    assert g.muestrear(None) is False
    assert g.muestrear({"temperatura": 20.0, "humedad": 50.0, "gas": 0.0}) is True
    assert list(g.registros[0]) == ["fecha_hora", "temperatura", "humedad", "gas"]
    assert len(g.registros) == 1


def test_las_tres_tasas_respetan_el_limite_del_dht11():
    # El DHT11 entrega 1 dato por segundo: ninguna tasa puede ser más rápida que eso.
    assert sorted(sensor_arduino.TASAS.values()) == [1.0, 2.0, 3.0]


def test_grabadora_registra_inicio_cierre_y_duracion():
    inicio = datetime(2026, 11, 23, 14, 5, 12)
    g = Grabadora(ahora=inicio)
    assert g.inicio == inicio and g.cierre is None
    assert g.duracion(ahora=inicio + timedelta(seconds=5)) == 5.0   # en curso: mide hasta "ahora"
    g.cerrar(ahora=inicio + timedelta(seconds=90))
    g.cerrar(ahora=inicio + timedelta(seconds=999))                 # un segundo cierre no cambia nada
    assert g.cierre == inicio + timedelta(seconds=90)
    assert g.duracion() == 90.0


# --- Ventana: Iniciar -> Detener transmisión -> servidor ---------------------------------------------------

class LectorFalso:
    puerto = "COM_FALSO"

    def ultima(self):
        return {"temperatura": 22.5, "humedad": 55.0, "gas": 3.0}

    def detener(self):
        pass


@pytest.fixture
def ventana():
    try:
        v = VentanaCliente()
    except tk.TclError:
        pytest.skip("No hay pantalla disponible para Tkinter")
    v.withdraw()
    yield v
    v.destroy()


def test_rec_stop_envia_las_filas_al_servidor(ventana, tmp_path):
    escucha = socket.create_server(("127.0.0.1", 0))
    resultado = {}

    def servir():
        conexion, _ = escucha.accept()
        with conexion:
            resultado.update(servidor_prueba.atender(conexion))
    threading.Thread(target=servir, daemon=True).start()

    v = ventana
    v.var_ip.set("127.0.0.1")
    v.var_puerto.set(str(escucha.getsockname()[1]))
    v.lector = LectorFalso()
    v.iniciar_grabacion()                 # toma la 1ª muestra de inmediato
    assert v.grabando and str(v.boton_detener.cget("state")) == "normal"
    v._parar_muestreo()
    v._tomar_muestra()                    # 2ª y 3ª muestra sin esperar el reloj
    v._parar_muestreo()
    v._tomar_muestra()
    v.detener_grabacion()
    limite = time.monotonic() + 5
    while v.emisor is not None and time.monotonic() < limite:
        v.update()
        time.sleep(0.02)
    escucha.close()
    assert resultado.get("ok") and resultado["filas"] == 3
    assert set(resultado["registros"][0]) == {"fecha_hora", "temperatura", "humedad", "gas"}
    assert "confirmó" in v.etiqueta_envio.cget("text")
    assert not v.grabando
    traza = v.etiqueta_traza.cget("text")             # trazabilidad: inicio y cierre quedan a la vista
    assert "Inicio: " in traza and "Cierre: " in traza and "en curso" not in traza
    assert "3 filas" in traza and "1 dato por segundo" in traza
    # Exportar: el JSON es idéntico a lo que recibió el servidor; el CSV trae las mismas filas.
    assert str(v.boton_csv.cget("state")) == "normal" and str(v.boton_json.cget("state")) == "normal"
    v.exportar_captura("json", tmp_path / "t.json")
    v.exportar_captura("csv", tmp_path / "t.csv")
    assert json.loads((tmp_path / "t.json").read_text(encoding="utf-8")) == resultado["registros"]
    with open(tmp_path / "t.csv", encoding="utf-8", newline="") as archivo:
        filas = list(csv.DictReader(archivo))
    assert list(filas[0]) == ["fecha_hora", "temperatura", "humedad", "gas"]
    assert [f["gas"] for f in filas] == [str(r["gas"]) for r in resultado["registros"]]
    assert "3 filas guardadas" in v.etiqueta_exportado.cget("text")


def test_stop_sin_filas_no_envia(ventana):
    v = ventana
    v.lector = None                       # sin sensor: no hay lectura que grabar
    v.iniciar_grabacion()
    v.detener_grabacion()
    assert v.emisor is None
    assert "ninguna fila" in v.etiqueta_rec.cget("text")


def test_rec_exige_ip_y_puerto_validos(ventana):
    v = ventana
    v.var_puerto.set("abc")
    v.iniciar_grabacion()
    assert not v.grabando


def test_desconectar_en_plena_transmision_deja_la_traza_interrumpida(ventana):
    v = ventana
    v.lector = LectorFalso()
    v.iniciar_grabacion()
    assert "en curso" in v.etiqueta_traza.cget("text")
    v.desconectar_arduino("Sin conexión")
    traza = v.etiqueta_traza.cget("text")
    assert "Interrumpida (no se envió)" in traza and not v.grabando


def test_exportar_deshabilitado_sin_transmision_y_durante_ella(ventana, tmp_path):
    v = ventana
    assert str(v.boton_csv.cget("state")) == "disabled"      # aún no hay nada que exportar
    v.exportar_captura("csv", tmp_path / "x.csv")             # no hace nada
    assert not (tmp_path / "x.csv").exists()
    v.lector = LectorFalso()
    v.iniciar_grabacion()
    assert str(v.boton_json.cget("state")) == "disabled"     # mientras se transmite
    v.desconectar_arduino()                                   # corte: lo capturado queda exportable
    assert str(v.boton_json.cget("state")) == "normal"
    v.exportar_captura("json", tmp_path / "x.json")
    assert len(json.loads((tmp_path / "x.json").read_text(encoding="utf-8"))) == 1


def test_exportaciones_tienen_solo_las_cuatro_columnas_enviadas(tmp_path):
    filas = [{"fecha_hora": "2026-11-23 15:00:00.123", "temperatura": 23.0, "humedad": 55.0, "gas": 134.0}]
    sensor_arduino.exportar_csv(tmp_path / "a.csv", filas)
    sensor_arduino.exportar_json(tmp_path / "a.json", filas)
    assert (tmp_path / "a.csv").read_text(encoding="utf-8").splitlines() == [
        "fecha_hora,temperatura,humedad,gas", "2026-11-23 15:00:00.123,23.0,55.0,134.0"]
    assert json.loads((tmp_path / "a.json").read_text(encoding="utf-8")) == filas
