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


# --- Semáforo y ecualizador -------------------------------------------------------------------

@pytest.mark.parametrize("campo, valor, esperado", [
    ("temperatura", -0.1, "rojo"), ("temperatura", 0, "amarillo"), ("temperatura", 4.9, "amarillo"),
    ("temperatura", 5, "verde"), ("temperatura", 25, "verde"), ("temperatura", 45, "verde"),
    ("temperatura", 45.1, "amarillo"), ("temperatura", 50, "amarillo"), ("temperatura", 50.1, "rojo"),
    ("humedad", 19.9, "rojo"), ("humedad", 20, "amarillo"), ("humedad", 26.9, "amarillo"),
    ("humedad", 27, "verde"), ("humedad", 55, "verde"), ("humedad", 83, "verde"),
    ("humedad", 83.1, "amarillo"), ("humedad", 90, "amarillo"), ("humedad", 90.1, "rojo"),
    ("gas", 0, "verde"), ("gas", 299, "verde"), ("gas", 300, "amarillo"),
    ("gas", 599, "amarillo"), ("gas", 600, "rojo"), ("gas", 1023, "rojo"),
])
def test_semaforo_respeta_los_limites_del_dht11_y_los_umbrales_del_gas(campo, valor, esperado):
    assert sensor_arduino.nivel(campo, valor) == esperado


def test_escala_de_la_barra_deja_espacio_para_la_zona_roja():
    assert sensor_arduino.escala("temperatura") == (-5.0, 55.0)     # 0-50 °C más 10 % por lado
    assert sensor_arduino.escala("humedad") == (13.0, 97.0)         # 20-90 % más 10 % por lado
    assert sensor_arduino.escala("gas") == (0.0, 1023.0)


def test_ecualizador_colorea_los_segmentos_con_el_semaforo(ventana):
    temperatura = ventana.graficos["temperatura"]
    assert temperatura.niveles_segmento == (["rojo"] + ["amarillo"] * 2 + ["verde"] * 10
                                            + ["amarillo"] * 2 + ["rojo"])
    gas = ventana.graficos["gas"]
    assert gas.niveles_segmento[0] == "verde" and gas.niveles_segmento[-1] == "rojo"


def test_ecualizador_enciende_segmentos_segun_el_valor(ventana):
    t = ventana.graficos["temperatura"]
    assert t._encendidos(25) == 8                 # mitad de la escala
    assert t._encendidos(-50) == 1                # siempre se ve al menos un segmento
    assert t._encendidos(500) == t.SEGMENTOS      # fuera de la escala por arriba: barra completa


# Propósito: dejar correr la animación de la barra hasta que llegue a su nivel (con tope de tiempo).
def _terminar_animacion(ventana, grafico, tiempo=3.0):
    limite = time.monotonic() + tiempo
    while grafico._id_animacion is not None and time.monotonic() < limite:
        ventana.update()
        time.sleep(0.01)


def test_ecualizador_dibuja_una_sola_barra_sin_bordes(ventana):
    t = ventana.graficos["temperatura"]
    t.winfo_width, t.winfo_height = (lambda: 400), (lambda: 170)    # ventana oculta: simula el tamaño
    t.agregar(25)
    _terminar_animacion(ventana, t)
    rectangulos = [i for i in t.find_all() if t.type(i) == "rectangle"]
    assert len(rectangulos) == t.SEGMENTOS                          # una barra: un rectángulo por segmento
    assert all(t.itemcget(i, "outline") == "" for i in rectangulos)  # sin bordes negros
    lit = [i for i in rectangulos if t.itemcget(i, "fill") in sensor_arduino_colores()]
    assert len(lit) == 8                                            # 25 °C: mitad de la escala


def sensor_arduino_colores():
    from cliente import COLORES_NIVEL
    return set(COLORES_NIVEL.values())


def test_la_barra_sube_y_baja_hasta_el_nivel_de_cada_lectura(ventana):
    t = ventana.graficos["temperatura"]
    t.winfo_width, t.winfo_height = (lambda: 400), (lambda: 170)
    t.agregar(25)
    _terminar_animacion(ventana, t)
    assert t._nivel_mostrado == 8
    t.agregar(45)                                   # sube
    ventana.update()
    assert 8 <= t._nivel_mostrado < t._encendidos(45)    # va en camino, todavía no llega
    _terminar_animacion(ventana, t)
    assert t._nivel_mostrado == t._encendidos(45) == 14
    t.agregar(5)                                    # baja
    _terminar_animacion(ventana, t)
    assert t._nivel_mostrado == t._encendidos(5) == 3


def test_ecualizador_guarda_historial_acotado_y_limpiar_vuelve_a_cero(ventana):
    t = ventana.graficos["humedad"]
    t.winfo_width, t.winfo_height = (lambda: 400), (lambda: 170)
    for i in range(t.HISTORIAL + 20):
        t.agregar(50 + i * 0.01)
    assert len(t.puntos) == t.HISTORIAL
    t.limpiar()
    assert not t.puntos and t._nivel_mostrado == 0 and t._id_animacion is None
