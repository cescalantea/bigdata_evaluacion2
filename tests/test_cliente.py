"""Pruebas del cliente. Ejecutar desde la carpeta del proyecto:  python -m pytest -q  (requiere pytest)"""

import gc
import json
import socket
import sys
import threading
import time
import tkinter as tk
from pathlib import Path

import pandas as pd
import pytest

import servidor_prueba
from cliente import (Dataset, EmisorTCP, EnvioCancelado, ErrorArchivo, ErrorCliente, ErrorRed,
                     VentanaCliente)

CSV_REAL = Path(__file__).parent.parent / "CAMELS_CL_caudales_2015_2019_569250.csv"


# Propósito: un CSV pequeño con tildes y un vacío.
@pytest.fixture
def csv_chico(tmp_path):
    ruta = tmp_path / "datos.csv"
    ruta.write_text(
        "fecha,estacion,zona,caudal,nota\n"
        "2015-01-01,Río Ñuble,Norte,1.5,a\n"
        "2015-01-02,Río Ñuble,Norte,-2.0,b\n"
        "2016-05-05,Estero Sur,Centro,,c\n"
        "2017-07-07,Canal Este,Sur,4.0,d\n",
        encoding="utf-8")
    return ruta


# Propósito: levantar un servidor en un hilo. 'atender' es la función que procesa al cliente.
def levantar_servidor(atender):
    escucha = socket.create_server(("127.0.0.1", 0))
    resultado = {}

    def tarea():
        conexion, _ = escucha.accept()
        with conexion:
            resultado.update(atender(conexion))
    hilo = threading.Thread(target=tarea, daemon=True)
    hilo.start()
    return escucha, escucha.getsockname()[1], resultado, hilo


@pytest.fixture
def servidor():
    escucha, puerto, resultado, hilo = levantar_servidor(servidor_prueba.atender)
    yield puerto, resultado, hilo
    escucha.close()


# --- Carga, informe y selección de columnas --------------------------------------

def test_carga_cualquier_csv_y_conserva_las_columnas(csv_chico):
    ds = Dataset()
    ds.cargar(csv_chico)
    assert list(ds.datos.columns) == ["fecha", "estacion", "zona", "caudal", "nota"]
    assert len(ds.datos) == 4


def test_informe_cuenta_vacios_y_duplicados(tmp_path):
    ruta = tmp_path / "d.csv"
    ruta.write_text("a,b\n1,x\n1,x\n2,\n", encoding="utf-8")
    ds = Dataset()
    ds.cargar(ruta)
    assert "Filas: 3" in ds.informe and "b=1" in ds.informe
    assert "Filas idénticas repetidas: 1" in ds.informe


@pytest.mark.parametrize("contenido", ["", "a,b\n", "a, a\n1,2\n"])
def test_archivos_inservibles_se_rechazan(tmp_path, contenido):
    ruta = tmp_path / "malo.csv"
    ruta.write_text(contenido, encoding="utf-8")
    with pytest.raises(ErrorArchivo):
        Dataset().cargar(ruta)


def test_archivo_inexistente(tmp_path):
    with pytest.raises(ErrorArchivo):
        Dataset().cargar(tmp_path / "no_existe.csv")


def test_seleccionar_devuelve_solo_las_columnas_elegidas(csv_chico):
    ds = Dataset()
    ds.cargar(csv_chico)
    resultado = ds.seleccionar(["caudal", "fecha"])
    assert list(resultado.columns) == ["caudal", "fecha"] and len(resultado) == 4


def test_seleccionar_sin_columnas_se_rechaza_antes_de_enviar(csv_chico):
    ds = Dataset()
    ds.cargar(csv_chico)
    with pytest.raises(ErrorCliente):
        ds.seleccionar([])


# --- Envío -------------------------------------------------------------------------

def test_envio_por_lotes_llega_completo_y_con_nulos(csv_chico, servidor):
    puerto, recibido, hilo = servidor
    ds = Dataset()
    ds.cargar(csv_chico)
    avances = []
    respuesta = EmisorTCP().enviar(ds.datos, "127.0.0.1", puerto, 3, lambda a, t: avances.append((a, t)))
    hilo.join(5)
    assert respuesta == {"ok": True, "filas": 4, "detalle": "Recepción correcta."}
    assert avances == [(1, 2), (2, 2)]
    assert recibido["registros"][2]["caudal"] is None            # el vacío viajó como null
    assert recibido["registros"][0]["estacion"] == "Río Ñuble"   # las tildes sobreviven


def test_solo_viajan_las_columnas_elegidas(csv_chico, servidor):
    puerto, recibido, hilo = servidor
    ds = Dataset()
    ds.cargar(csv_chico)
    EmisorTCP().enviar(ds.seleccionar(["fecha", "caudal"]), "127.0.0.1", puerto, 10)
    hilo.join(5)
    assert all(list(r) == ["fecha", "caudal"] for r in recibido["registros"])


def test_los_decimales_llegan_exactos(tmp_path, servidor):
    puerto, recibido, hilo = servidor
    ruta = tmp_path / "p.csv"
    ruta.write_text("area,lon\n467.556221617695,-69.255\n0.1,123456789.123456\n", encoding="utf-8")
    ds = Dataset()
    ds.cargar(ruta)
    EmisorTCP().enviar(ds.datos, "127.0.0.1", puerto, 1)
    hilo.join(5)
    assert [r["area"] for r in recibido["registros"]] == [467.556221617695, 0.1]
    assert [r["lon"] for r in recibido["registros"]] == [-69.255, 123456789.123456]


def test_servidor_detecta_envio_sin_marca_final():
    escucha, puerto, resultado, hilo = levantar_servidor(servidor_prueba.atender)
    with socket.create_connection(("127.0.0.1", puerto)) as s:
        s.sendall(b'[{"a":1}')          # cortado, sin ] ni <EOF>
        s.shutdown(socket.SHUT_WR)
        respuesta = json.loads(s.recv(4096))
    hilo.join(5)
    escucha.close()
    assert respuesta["ok"] is False and "EOF" in respuesta["detalle"]


def test_compatible_con_la_recepcion_del_demo_del_grupo_4(csv_chico):
    """Reproduce la lógica del servidor del demo: leer hasta el cierre, quitar <EOF>, json.loads.
    Ese servidor NO responde nada: el cliente debe terminar igual y sin error."""
    def como_el_demo(conexion):
        fragmentos = []
        while True:
            fragmento = conexion.recv(1048576).decode("utf-8")
            if not fragmento:
                break
            fragmentos.append(fragmento)
        datos = "".join(fragmentos).replace("<EOF>", "")
        return {"df": pd.DataFrame(json.loads(datos))}   # cierra sin responder

    escucha, puerto, resultado, hilo = levantar_servidor(como_el_demo)
    ds = Dataset()
    ds.cargar(csv_chico)
    respuesta = EmisorTCP().enviar(ds.datos, "127.0.0.1", puerto, 2)
    hilo.join(5)
    escucha.close()
    assert respuesta is None
    assert len(resultado["df"]) == 4 and list(resultado["df"].columns) == list(ds.datos.columns)


def test_error_claro_si_no_hay_servidor():
    libre = socket.create_server(("127.0.0.1", 0))
    puerto = libre.getsockname()[1]
    libre.close()  # ahora nadie escucha en ese puerto
    with pytest.raises(ErrorRed, match="No hay un servidor"):
        EmisorTCP().enviar(pd.DataFrame({"a": [1]}), "127.0.0.1", puerto, 1)


def test_no_envia_si_no_hay_filas():
    with pytest.raises(ErrorRed):
        EmisorTCP().enviar(pd.DataFrame({"a": []}), "127.0.0.1", 1, 1)


@pytest.mark.skipif(not CSV_REAL.exists(), reason="El CSV real no está en la carpeta")
def test_csv_real_completo_de_extremo_a_extremo(servidor):
    puerto, recibido, hilo = servidor
    ds = Dataset()
    ds.cargar(CSV_REAL)
    respuesta = EmisorTCP().enviar(ds.datos, "127.0.0.1", puerto, 5000)
    hilo.join(60)
    assert respuesta["ok"] is True and respuesta["filas"] == len(ds.datos) == 569_250


# --- Ventana del servidor de prueba (interruptor de escucha) ------------------------

# Propósito: liberar los objetos de Tkinter de una ventana ya cerrada DESDE EL HILO PRINCIPAL.
# Si los liberara el recolector de basura dentro de otro hilo, Tkinter se bloquearía 1 s por cada uno.
def limpiar_tk():
    gc.collect()


# Propósito: crear la ventana sin mostrarla; se omite la prueba si no hay pantalla.
@pytest.fixture
def ventana_servidor():
    try:
        ventana = servidor_prueba.ServidorPrueba()
    except tk.TclError:
        pytest.skip("No hay pantalla disponible para Tkinter")
    ventana.withdraw()
    yield ventana
    ventana._cerrar()
    limpiar_tk()


# Propósito: dejar que la ventana procese las tareas de los hilos hasta que se cumpla la condición.
def esperar(ventana, condicion, segundos=5):
    fin = time.time() + segundos
    while time.time() < fin and not condicion():
        ventana.update()
        time.sleep(0.02)
    return condicion()


# Propósito: un puerto libre para no chocar con otros servidores abiertos en el equipo.
def puerto_libre():
    with socket.create_server(("127.0.0.1", 0)) as s:
        return s.getsockname()[1]


def test_interruptor_inicia_y_detiene_la_escucha(ventana_servidor):
    puerto = puerto_libre()
    ventana_servidor.var_puerto.set(str(puerto))
    ventana_servidor.alternar()
    assert ventana_servidor.servidor is not None
    assert ventana_servidor.boton.cget("text") == "Detener escucha"
    socket.create_connection(("127.0.0.1", puerto), timeout=2).close()   # acepta conexiones
    ventana_servidor.alternar()
    assert "Deteniendo" in ventana_servidor.estado.cget("text")
    assert esperar(ventana_servidor, lambda: ventana_servidor.servidor is None)
    assert ventana_servidor.boton.cget("text") == "Iniciar escucha"
    socket.create_server(("127.0.0.1", puerto)).close()   # el puerto quedó libre: se puede reabrir


def test_puerto_ocupado_se_explica_sin_cerrar_el_programa(ventana_servidor):
    with socket.create_server(("127.0.0.1", 0)) as ocupado:
        ventana_servidor.var_puerto.set(str(ocupado.getsockname()[1]))
        ventana_servidor.alternar()
    assert ventana_servidor.servidor is None
    assert "ya está en uso" in ventana_servidor.estado.cget("text")


def test_se_puede_reiniciar_la_escucha(ventana_servidor):
    ventana_servidor.var_puerto.set(str(puerto_libre()))
    for _ in range(2):
        ventana_servidor.alternar()
        assert ventana_servidor.servidor is not None
        ventana_servidor.alternar()
        assert esperar(ventana_servidor, lambda: ventana_servidor.servidor is None)


def test_envio_a_traves_de_la_ventana_queda_en_el_registro(csv_chico, ventana_servidor):
    puerto = puerto_libre()
    ventana_servidor.var_puerto.set(str(puerto))
    ventana_servidor.alternar()
    ds = Dataset()
    ds.cargar(csv_chico)
    respuesta = EmisorTCP().enviar(ds.datos, "127.0.0.1", puerto, 2)
    esperar(ventana_servidor, lambda: "filas recibidas" in ventana_servidor.registro.get("1.0", "end"))
    assert respuesta["ok"] is True
    assert "4 filas recibidas" in ventana_servidor.registro.get("1.0", "end")


# --- Ventana del cliente: casillas de columnas -----------------------------------------

# Propósito: ventana del cliente con un CSV pequeño ya cargado (sin pasar por el hilo de carga).
@pytest.fixture
def ventana_cliente(csv_chico):
    try:
        ventana = VentanaCliente()
    except tk.TclError:
        pytest.skip("No hay pantalla disponible para Tkinter")
    ventana.withdraw()
    ventana.dataset.cargar(csv_chico)
    ventana.cargado = True
    ventana._crear_casillas()
    ventana._bloquear(False)   # lo que hace _al_cargar: habilitar los botones
    yield ventana
    ventana.destroy()
    limpiar_tk()


def test_hay_una_casilla_por_columna_y_todas_marcadas(ventana_cliente):
    v = ventana_cliente
    assert list(v.var_columnas) == ["fecha", "estacion", "zona", "caudal", "nota"]
    assert all(var.get() for var in v.var_columnas.values())
    assert len(v.mostrar_seleccion().columns) == 5


def test_desmarcar_una_columna_la_quita_de_la_seleccion_y_la_vista_previa(ventana_cliente):
    v = ventana_cliente
    v.var_columnas["nota"].set(False)
    v.var_columnas["zona"].set(False)
    resultado = v.mostrar_seleccion()
    assert list(resultado.columns) == ["fecha", "estacion", "caudal"]
    assert list(v.tabla["columns"]) == ["fecha", "estacion", "caudal"]
    assert len(v.tabla.get_children()) == 4
    assert v.etiqueta_seleccion.cget("text").startswith("3 de 5 columnas")


def test_marcar_y_desmarcar_todas(ventana_cliente):
    v = ventana_cliente
    v.marcar_todas(False)
    assert v.mostrar_seleccion() is None
    assert "al menos una columna" in v.etiqueta_seleccion.cget("text")
    v.marcar_todas(True)
    assert len(v.mostrar_seleccion().columns) == 5


def test_no_se_envia_sin_columnas(ventana_cliente):
    v = ventana_cliente
    v.marcar_todas(False)
    v.enviar()
    assert "al menos una columna" in v.etiqueta_envio.cget("text")
    assert str(v.boton_enviar.cget("state")) == "normal"   # no quedó bloqueado


def test_cargar_otro_csv_rearma_las_casillas(ventana_cliente, tmp_path):
    v = ventana_cliente
    otro = tmp_path / "otro.csv"
    otro.write_text("pais,ventas\nChile,10\nPerú,7\n", encoding="utf-8")
    nuevo = Dataset()
    nuevo.cargar(otro)
    v._al_cargar(nuevo)
    assert list(v.var_columnas) == ["pais", "ventas"]


# --- Cancelar el envío ---------------------------------------------------------------

# Propósito: un servidor que acepta la conexión pero nunca lee ni responde (como uno "colgado").
@pytest.fixture
def servidor_mudo():
    escucha = socket.create_server(("127.0.0.1", 0))
    conexiones = []
    threading.Thread(target=lambda: conexiones.append(escucha.accept()[0]), daemon=True).start()
    yield escucha.getsockname()[1]
    for c in conexiones:
        c.close()
    escucha.close()


# Propósito: ejecutar emisor.enviar en un hilo y devolver (hilo, resultado) para inspeccionarlo.
def enviar_en_hilo(emisor, datos, puerto):
    resultado = {}

    def tarea():
        try:
            resultado["respuesta"] = emisor.enviar(datos, "127.0.0.1", puerto, 1000)
        except Exception as error:   # lo revisa la prueba
            resultado["error"] = error
    hilo = threading.Thread(target=tarea, daemon=True)
    hilo.start()
    return hilo, resultado


@pytest.mark.skipif(sys.platform != "win32",
                    reason="En Windows rechazar un puerto cerrado tarda ~2 s; en otros sistemas es inmediato")
def test_cancelar_mientras_conecta_corta_la_espera():
    libre = socket.create_server(("127.0.0.1", 0))
    puerto = libre.getsockname()[1]
    libre.close()   # nadie escucha: el cliente queda reintentando la conexión
    emisor = EmisorTCP()
    hilo, resultado = enviar_en_hilo(emisor, pd.DataFrame({"a": [1]}), puerto)
    time.sleep(0.4)
    inicio = time.time()
    emisor.cancelar()
    hilo.join(3)
    assert isinstance(resultado.get("error"), EnvioCancelado)
    assert time.time() - inicio < 1.5


@pytest.mark.parametrize("filas", [10, 300_000])   # chico: espera la respuesta; grande: se traba enviando
def test_cancelar_con_un_servidor_que_no_responde(servidor_mudo, filas):
    datos = pd.DataFrame({"a": ["x" * 20] * filas, "b": range(filas)})
    emisor = EmisorTCP()
    hilo, resultado = enviar_en_hilo(emisor, datos, servidor_mudo)
    time.sleep(0.6)
    assert hilo.is_alive()              # sigue esperando: el servidor no hace nada
    inicio = time.time()
    emisor.cancelar()
    hilo.join(3)
    assert isinstance(resultado.get("error"), EnvioCancelado)    # y NO se confunde con un éxito
    assert "respuesta" not in resultado
    assert time.time() - inicio < 1.5


def test_cancelar_sin_envio_en_curso_no_falla():
    EmisorTCP().cancelar()


def test_ventana_cancelar_envio_deja_todo_listo_para_reintentar(ventana_cliente, servidor_mudo):
    v = ventana_cliente
    assert str(v.boton_cancelar.cget("state")) == "disabled"     # sin envío no se puede cancelar
    v.var_puerto.set(str(servidor_mudo))
    v.enviar()
    assert str(v.boton_cancelar.cget("state")) == "normal"
    assert str(v.boton_enviar.cget("state")) == "disabled"
    esperar(v, lambda: False, segundos=0.5)                       # dejar que se conecte y espere
    v.cancelar_envio()
    assert esperar(v, lambda: "cancelado" in v.etiqueta_envio.cget("text").lower())
    assert v.emisor is None
    assert str(v.boton_enviar.cget("state")) == "normal"
    assert str(v.boton_cancelar.cget("state")) == "disabled"
