"""Regresión del servidor Grupo 4: sockets reales y fragmentación determinista."""
import gc
import json
import socket
import sys
import threading
import time
import types
from pathlib import Path

import pandas as pd
import pytest
import servidor_prueba as s
from cliente import EmisorTCP, VentanaCliente


class Fragmentos:
    def __init__(self, partes):
        self.partes = iter(partes)

    def settimeout(self, valor):
        pass

    def recv(self, cantidad):
        return next(self.partes, b"")


def recibir(partes):
    return s.decodificar(s.recibir_todo(Fragmentos(partes)))


def test_fragmentacion_y_datos():
    esperado = [{"texto": "ñ a<EOF>b", "numero": 9007199254740993, "nulo": None}]
    datos = json.dumps(esperado, ensure_ascii=False).encode() + b"<EOF>"
    assert recibir([bytes([b]) for b in datos]) == esperado
    df = s.dataframe_validado(esperado + [{"numero": None}])
    assert df.loc[0, "numero"] == 9007199254740993
    assert df.loc[1, "numero"] is None
    assert list(df.columns) == ["texto", "numero", "nulo"]


@pytest.mark.parametrize("datos", [
    b"", b'[{"a":1}]', b'[{"a":1}<EOF>', b'bad<EOF>',
    b'{"a":[1]}<EOF>', b'[1]<EOF>', b'[NaN]<EOF>',
    b'[{"a":1,"a":2}]<EOF>', b'["\xff"]<EOF>', b'[{"a":1e400}]<EOF>',
])
def test_rechazos(datos):
    with pytest.raises((ValueError, UnicodeError)):
        recibir([datos])


def test_puerto_sensores_reconstruye_json_lines_fragmentados():
    mensajes = (
        b'{"type":"SENSOR","timestamp":"2026-11-14T14:32:10",'
        b'"temperature":23.8,"humidity":56.4,"gas":412}\n'
        b'{"type":"SENSOR","temp":24.1,"hum":55.9,"gas":418}\n'
    )
    sensores = []
    cantidad = s.recibir_sensores(
        Fragmentos([bytes([byte]) for byte in mensajes]),
        lambda mensaje, conexion: sensores.append(mensaje),
    )
    assert cantidad == 2
    assert [m["gas"] for m in sensores] == [412, 418]


def test_lectura_arduino_se_confirma_sin_guardarse_en_mongo(ventana):
    class ConexionFalsa:
        def __init__(self):
            self.respuesta = b""

        def settimeout(self, valor):
            pass

        def sendall(self, datos):
            self.respuesta += datos

    conexion = ConexionFalsa()
    ventana._atender_sensor(
        {"type": "SENSOR", "temp": 23.8, "hum": 56.4, "gas": 412},
        conexion,
    )

    respuesta = json.loads(conexion.respuesta)
    assert respuesta["ok"] is True
    assert respuesta["persisted"] is False
    assert respuesta["sensores"] == 1


def test_mongo_guarda_las_columnas_csv_en_la_raiz(monkeypatch):
    class ColeccionFalsa:
        def __init__(self):
            self.lotes = []

        def insert_many(self, documentos, ordered):
            self.lotes.append((documentos, ordered))

    class BaseFalsa:
        def __init__(self, coleccion):
            self.coleccion = coleccion

        def __getitem__(self, nombre):
            assert nombre == "datos_csv"
            return self.coleccion

    monkeypatch.setenv("MONGODB_URI", "mongodb://localhost")
    coleccion = ColeccionFalsa()
    repo = s.MongoRepositorio()
    repo._db = BaseFalsa(coleccion)
    repo.tamano_lote = 2
    monkeypatch.setattr(repo, "conectar", lambda: True)
    dataframe = pd.DataFrame([
        {"fecha": "2015-01-01", "caudal": 1.5},
        {"fecha": "2015-01-02", "caudal": None},
    ], dtype=object)

    resultado = repo.guardar_dataframe(dataframe)

    assert resultado["insertados"] == 2
    assert [documento for lote, _ in coleccion.lotes for documento in lote] == [
        {"fecha": "2015-01-01", "caudal": 1.5},
        {"fecha": "2015-01-02", "caudal": None},
    ]
    assert all(ordered is False for _, ordered in coleccion.lotes)


def test_mongo_reintenta_ping_con_espera(monkeypatch):
    class ErrorMongo(Exception):
        pass

    class ClienteFalso:
        def __init__(self, falla):
            self.falla = falla
            self.cerrado = False
            self.admin = self

        def command(self, comando):
            assert comando == "ping"
            if self.falla:
                raise ErrorMongo("conexión caída")

        def close(self):
            self.cerrado = True

        def __getitem__(self, nombre):
            assert nombre == "bigdata_eval3"
            return object()

    instancias = [ClienteFalso(True), ClienteFalso(False)]
    clientes = iter(instancias)
    modulo = types.ModuleType("pymongo")
    modulo.MongoClient = lambda *args, **kwargs: next(clientes)
    errores = types.ModuleType("pymongo.errors")
    errores.PyMongoError = ErrorMongo
    modulo.errors = errores
    monkeypatch.setitem(sys.modules, "pymongo", modulo)
    monkeypatch.setitem(sys.modules, "pymongo.errors", errores)
    monkeypatch.setenv("MONGODB_URI", "mongodb://localhost")
    monkeypatch.setenv("MONGODB_MAX_ATTEMPTS", "2")
    monkeypatch.setenv("MONGODB_RETRY_DELAY", "0.25")
    esperas = []
    monkeypatch.setattr(s.time, "sleep", esperas.append)
    repo = s.MongoRepositorio()

    assert repo.conectar() is True
    assert esperas == [0.25]
    assert instancias[0].cerrado is True
    assert repo._cliente is not None
    assert repo._db is not None


def test_limite_y_cancelacion(monkeypatch):
    monkeypatch.setattr(s, "MAX_BYTES", 4)
    with pytest.raises(ValueError, match="máximo"):
        recibir([b"12345"])
    detener = threading.Event()
    detener.set()
    with pytest.raises(ValueError, match="cancelada"):
        s.recibir_todo(Fragmentos([]), detener)


def test_timeout_real(monkeypatch):
    monkeypatch.setattr(s, "TIEMPO_ESPERA", .05)
    with socket.create_server(("127.0.0.1", 0)) as escucha:
        with socket.create_connection(escucha.getsockname()) as cliente:
            conexion, _ = escucha.accept()
            with conexion, pytest.raises(TimeoutError):
                s.recibir_todo(conexion)


def test_duracion_total_real(monkeypatch):
    monkeypatch.setattr(s, "TIEMPO_TOTAL", .05)
    with socket.create_server(("127.0.0.1", 0)) as escucha:
        with socket.create_connection(escucha.getsockname()) as cliente:
            conexion, _ = escucha.accept()
            with conexion, pytest.raises(TimeoutError):
                # Hay datos disponibles pero el plazo total sigue aplicándose.
                cliente.sendall(b"[")
                s.recibir_todo(conexion)


def esperar(w, condicion, segundos=30):
    fin = time.monotonic() + segundos
    while time.monotonic() < fin:
        w.update()
        if condicion():
            return
        time.sleep(.005)
    pytest.fail("Tiempo agotado esperando al servidor")


@pytest.fixture
def ventana():
    try:
        w = s.ServidorBigData()
    except s.tk.TclError as error:
        if "no display" in str(error) or "couldn't connect to display" in str(error):
            pytest.skip(f"Tkinter no dispone de pantalla: {error}")
        raise
    w.withdraw()
    yield w
    w._cerrar()
    fin = time.monotonic() + 5
    while time.monotonic() < fin:
        try:
            if not w.winfo_exists():
                break
            w.update()
        except s.tk.TclError:
            break
        time.sleep(.01)
    del w
    gc.collect()


def iniciar(w):
    with socket.create_server(("127.0.0.1", 0)) as libre:
        puerto = libre.getsockname()[1]
    with socket.create_server(("127.0.0.1", 0)) as libre:
        puerto_sensores = libre.getsockname()[1]
    w.var_puerto.set(str(puerto))
    w.var_puerto_sensores.set(str(puerto_sensores))
    w.iniciar()
    assert w.servidor is not None
    return puerto


def en_hilo(funcion):
    resultado = {}
    def ejecutar():
        try:
            resultado["valor"] = funcion()
        except Exception as error:
            resultado["error"] = error
    hilo = threading.Thread(target=ejecutar)
    hilo.start()
    return hilo, resultado


def terminar(w, trabajo, segundos=30):
    hilo, resultado = trabajo
    esperar(w, lambda: not hilo.is_alive(), segundos)
    hilo.join()
    if "error" in resultado:
        raise resultado["error"]
    return resultado["valor"]


def enviar_crudo(puerto, datos):
    with socket.create_connection(("127.0.0.1", puerto), timeout=5) as c:
        c.sendall(datos)
        c.shutdown(socket.SHUT_WR)
        partes = []
        while bloque := c.recv(4096):
            partes.append(bloque)
        return json.loads(b"".join(partes))


def test_cliente_real_consecutivas_integridad(ventana):
    puerto = iniciar(ventana)
    for registros in [
        [{"texto": "ñ a<EOF>b", "n": 9007199254740993}, {"texto": None, "n": None}],
        [{"fecha": "2015-01-01", "decimal": 467.556221617695}],
    ]:
        df = pd.DataFrame(registros, dtype=object)
        ack = terminar(ventana, en_hilo(lambda: EmisorTCP().enviar(df, "127.0.0.1", puerto, 1)))
        assert ack == {"ok": True, "filas": len(df)}
        assert ventana.df_recibido.to_dict("records") == registros
        assert list(ventana.tabla["columns"]) == list(df.columns)


def test_envio_csv_de_cliente_llega_al_repositorio_mongo(ventana):
    class RepositorioFalso:
        def __init__(self):
            self.registros = None

        def guardar_dataframe(self, dataframe):
            self.registros = dataframe.to_dict("records")
            return {
                "habilitado": True,
                "insertados": len(dataframe),
                "transferencia_id": "transferencia-prueba",
            }

        def cerrar(self):
            pass

    repositorio = RepositorioFalso()
    ventana.mongo = repositorio
    puerto = iniciar(ventana)
    registros = [
        {"fecha": "2015-01-01", "estacion": "Río Ñuble", "caudal": 1.5},
        {"fecha": "2015-01-02", "estacion": "Río Ñuble", "caudal": None},
    ]
    dataframe = pd.DataFrame(registros, dtype=object)

    respuesta = terminar(
        ventana,
        en_hilo(lambda: EmisorTCP().enviar(dataframe, "127.0.0.1", puerto, 1)),
    )

    assert respuesta == {"ok": True, "filas": 2}
    assert repositorio.registros == registros


def test_ack_negativo_cliente_y_recuperacion(ventana):
    puerto = iniciar(ventana)
    for datos in [b'[{"a":1}]', b'[{"a":1}', b'bad<EOF>', b'{"a":[1]}<EOF>']:
        respuesta = terminar(ventana, en_hilo(lambda: enviar_crudo(puerto, datos)))
        assert respuesta["ok"] is False and respuesta["filas"] == 0
        assert respuesta["detalle"]
    # El lector real de Grupo 3 conserva el ACK negativo, no lo convierte en None.
    with socket.create_server(("127.0.0.1", 0)) as escucha:
        def rechazar():
            c, _ = escucha.accept()
            with c:
                while c.recv(4096):
                    pass
                s.responder(c, {"ok": False, "filas": 0, "detalle": "rechazado"})
        hilo = threading.Thread(target=rechazar)
        hilo.start()
        respuesta = EmisorTCP().enviar(pd.DataFrame({"a": [1]}), "127.0.0.1", escucha.getsockname()[1], 1)
        hilo.join(5)
    assert respuesta["ok"] is False
    v = VentanaCliente()
    v.withdraw()
    try:
        v._al_enviar(1, respuesta, 0.1)
        assert "Respuesta inesperada" in v.etiqueta_envio.cget("text")
    finally:
        for identificador in v.tk.call("after", "info"):
            v.after_cancel(identificador)
        v.destroy()
        del v
        gc.collect()
    assert terminar(ventana, en_hilo(lambda: enviar_crudo(puerto, b'[{"ok":1}]<EOF>')))["ok"]
    assert ventana.df_recibido.to_dict("records") == [{"ok": 1}]


def test_simultaneas_detencion_reinicio(ventana):
    puerto = iniciar(ventana)
    puerto_sensores = int(ventana.var_puerto_sensores.get())
    c = socket.create_connection(("127.0.0.1", puerto), timeout=5)
    try:
        c.sendall(b'[{"a":')
        esperar(ventana, lambda: ventana._hilo_cliente is not None)
        with socket.create_connection(("127.0.0.1", puerto_sensores), timeout=5) as sensor:
            sensor.sendall(b'{"type":"SENSOR","temperature":23.8,"humidity":56.4,"gas":412}\n')
            sensor.shutdown(socket.SHUT_WR)
            respuesta = json.loads(sensor.recv(4096))
        assert respuesta["ok"] and respuesta["type"] == "SENSOR_ACK"
        ventana.detener()
        esperar(ventana, lambda: ventana.servidor is None)
        assert not ventana._hilos_clientes
        assert not ventana._hilo_escucha.is_alive()
        assert not ventana._hilo_escucha_sensores.is_alive()
        assert not json.loads(c.recv(4096))["ok"]
    finally:
        c.close()
    ventana.iniciar()
    assert terminar(ventana, en_hilo(lambda: enviar_crudo(puerto, b'[]<EOF>')))["ok"]


def test_cola_resiste_excepcion(ventana, caplog):
    aplicado = []
    def fallar():
        raise RuntimeError("fallo deliberado")
    ventana.cola.put(fallar)
    ventana.cola.put(lambda: aplicado.append(1))
    esperar(ventana, lambda: aplicado == [1])
    ventana.cola.put(lambda: aplicado.append(2))
    esperar(ventana, lambda: aplicado == [1, 2])
    assert "fallo deliberado" in caplog.text


def test_cancelacion_cliente_real(ventana):
    puerto = iniciar(ventana)
    original = pd.DataFrame({"anterior": [1]}, dtype=object)
    ventana._datos_recibidos(original)
    emisor = EmisorTCP()
    df = pd.DataFrame({"a": range(1000)})
    from cliente import EnvioCancelado
    trabajo = en_hilo(lambda: emisor.enviar(
        df, "127.0.0.1", puerto, 1, lambda a, t: emisor.cancelar()))
    esperar(ventana, lambda: not trabajo[0].is_alive())
    assert isinstance(trabajo[1].get("error"), EnvioCancelado)
    esperar(ventana, lambda: "Error en transferencia" in ventana.estado.cget("text"))
    assert ventana.df_recibido is original
    assert terminar(ventana, en_hilo(lambda: enviar_crudo(puerto, b'[{"a":2}]<EOF>')))["ok"]


def test_no_ack_positivo_si_dashboard_rechaza(ventana, monkeypatch):
    puerto = iniciar(ventana)
    def fallar(df, demora=0.0):
        raise RuntimeError("Dashboard no disponible")
    monkeypatch.setattr(ventana, "_datos_recibidos", fallar)
    ack = terminar(ventana, en_hilo(lambda: enviar_crudo(puerto, b'[{"a":1}]<EOF>')))
    assert ack["ok"] is False and "Dashboard" in ack["detalle"]
    assert ventana.df_recibido is None


def test_aceptacion_vencida_no_aplica_despues(ventana, monkeypatch):
    puerto = iniciar(ventana)
    monkeypatch.setattr(s, "TIEMPO_ESPERA", .05)
    trabajo = en_hilo(lambda: enviar_crudo(puerto, b'[{"a":1}]<EOF>'))
    # Sin bombear Tk durante el plazo: la tarea de aceptación debe expirar.
    trabajo[0].join(3)
    assert not trabajo[0].is_alive()
    assert not trabajo[1]["valor"]["ok"]
    esperar(ventana, lambda: ventana.cola.empty())
    assert ventana.df_recibido is None


def test_filtro_asincrono_descarta_resultado_antiguo(ventana, monkeypatch):
    inicio, continuar = threading.Event(), threading.Event()
    original = s.filtrar_dataframe
    def lento(df, texto):
        inicio.set()
        assert continuar.wait(5)
        return original(df, texto)
    monkeypatch.setattr(s, "filtrar_dataframe", lento)
    ventana._datos_recibidos(pd.DataFrame({"a": ["antes"]}))
    ventana.entry_filtro.insert(0, "antes")
    ventana.aplicar_filtro()
    esperar(ventana, inicio.is_set)
    latido = []
    ventana.after(1, lambda: latido.append(True))
    esperar(ventana, lambda: bool(latido))
    nuevo = pd.DataFrame({"b": ["después"]})
    ventana._datos_recibidos(nuevo)
    continuar.set()
    esperar(ventana, lambda: not ventana._hilo_filtro.is_alive())
    esperar(ventana, lambda: ventana.cola.empty())
    assert ventana.df_actual is nuevo
    assert list(ventana.tabla["columns"]) == ["b"]


def test_paginacion_filtros_y_dataset_vacio(ventana):
    df = pd.DataFrame({"texto": ["a.b", "[", "otro"] + ["resto"] * 1998})
    ventana._datos_recibidos(df)
    assert len(ventana.tabla.get_children()) == 1000
    ventana.pagina_siguiente()
    assert len(ventana.tabla.get_children()) == 1000
    ventana.pagina_siguiente()
    assert len(ventana.tabla.get_children()) == 1
    ventana.pagina_siguiente()
    assert ventana.pagina_actual == 2
    ventana.pagina_anterior()
    assert ventana.pagina_actual == 1
    for texto, cantidad in [("[", 1), (".", 1), ("missing", 0), ("RESTO", 1998)]:
        ventana.entry_filtro.delete(0, "end")
        ventana.entry_filtro.insert(0, texto)
        ventana.aplicar_filtro()
        esperar(ventana, lambda: "Filtro aplicado" in ventana.estado_datos.cget("text"))
        assert len(ventana.df_actual) == cantidad
        assert ventana.pagina_actual == 0
        if not cantidad:
            assert str(ventana.btn_anterior.cget("state")) == "disabled"
            assert str(ventana.btn_siguiente.cget("state")) == "disabled"
    ventana.limpiar_filtro()
    assert ventana.df_recibido is df and len(ventana.df_actual) == 2001
    ventana._datos_recibidos(pd.DataFrame())
    assert not ventana.tabla["columns"]
    assert not ventana.tabla.get_children()


def test_cierre_con_recepcion_activa(ventana):
    puerto = iniciar(ventana)
    with socket.create_connection(("127.0.0.1", puerto), timeout=5) as c:
        c.sendall(b"[")
        esperar(ventana, lambda: ventana._hilo_cliente is not None)
        ventana._cerrar()
        esperar(ventana, lambda: not ventana._hilo_escucha.is_alive())
    with socket.create_server(("127.0.0.1", puerto)):
        pass


def test_carga_masiva_sintetica(ventana, monkeypatch):
    n = 569250
    df = pd.DataFrame({
        "id": range(n), "fecha": ["2015-01-01"] * n,
        "estacion": ["Río sintético <EOF>"] * n, "zona": ["Centro"] * n,
        "caudal": [1.25] * n, "area": [467.556221617695] * n,
        "lon": [-69.255] * n, "nota": [None] * n,
    }, dtype=object)
    puerto = iniciar(ventana)
    avances = []
    medidas = {"bytes": 0, "send_s": 0}
    emisor = EmisorTCP()
    enviar = emisor._enviar_todo
    def medir_envio(conexion, datos):
        inicio = time.perf_counter()
        enviar(conexion, datos)
        medidas["send_s"] += time.perf_counter() - inicio
        medidas["bytes"] += len(datos)
    monkeypatch.setattr(emisor, "_enviar_todo", medir_envio)
    for nombre in ("decodificar", "dataframe_validado"):
        funcion = getattr(s, nombre)
        def medir(*args, funcion=funcion, nombre=nombre):
            inicio = time.perf_counter()
            resultado = funcion(*args)
            medidas[nombre + "_s"] = time.perf_counter() - inicio
            return resultado
        monkeypatch.setattr(s, nombre, medir)
    inicio = time.perf_counter()
    trabajo = en_hilo(lambda: emisor.enviar(
        df, "127.0.0.1", puerto, 5000, lambda a, t: avances.append((a, t))))
    ack = terminar(ventana, trabajo, 60)
    segundos = time.perf_counter() - inicio
    assert ack == {"ok": True, "filas": n}
    pd.testing.assert_frame_equal(ventana.df_recibido, df)
    assert len(ventana.tabla.get_children()) == 1000
    assert avances[-1] == (114, 114)
    print(f"\nCarga SINTETICA: filas={n}, columnas={list(df.columns)}, segundos_e2e={segundos:.3f}, ACK={ack}, df_MiB={df.memory_usage(deep=True).sum()/2**20:.2f}, medidas={medidas}, JSON_bytes={medidas['bytes'] - 5}")
    if __import__("sys").platform == "win32":
        import ctypes
        from ctypes import wintypes as W
        class Memoria(ctypes.Structure):
            _fields_ = [("cb", W.DWORD), ("faults", W.DWORD)] + [
                (campo, ctypes.c_size_t) for campo in ("peak", "working", "a", "b", "c", "d", "e", "f")]
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetCurrentProcess.restype = W.HANDLE
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        psapi.GetProcessMemoryInfo.argtypes = [W.HANDLE, ctypes.POINTER(Memoria), W.DWORD]
        memoria = Memoria()
        memoria.cb = ctypes.sizeof(memoria)
        assert psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(), ctypes.byref(memoria), memoria.cb)
        print(f"Proceso conjunto: RSS_MiB={memoria.working/2**20:.2f}, pico_RSS_MiB={memoria.peak/2**20:.2f}")
    latidos = []
    ventana.entry_filtro.insert(0, "<EOF>")
    ventana.after(1, lambda: latidos.append(True))
    inicio_filtro = time.perf_counter()
    ventana.aplicar_filtro()
    esperar(ventana, lambda: "Filtro aplicado" in ventana.estado_datos.cget("text"), 30)
    assert latidos and len(ventana.df_actual) == n
    assert len(ventana.tabla.get_children()) == 1000
    print(f"Filtro masivo literal: segundos={time.perf_counter()-inicio_filtro:.3f}, coincidencias={len(ventana.df_actual)}, latido_Tk={bool(latidos)}")


def test_dataset_real_si_disponible(ventana):
    archivo = Path("CAMELS_CL_caudales_2015_2019_569250.csv")
    if not archivo.exists():
        pytest.skip("CSV real ausente; la carga masiva disponible es sintética")
    from cliente import Dataset
    ds = Dataset()
    ds.cargar(archivo)
    puerto = iniciar(ventana)
    ack = terminar(ventana, en_hilo(lambda: EmisorTCP().enviar(ds.datos, "127.0.0.1", puerto, 5000)))
    assert ack == {"ok": True, "filas": 569250}


def test_estados_conexion_y_datos_separados(ventana):
    ventana._mostrar_estado("Servidor detenido", s.SUAVE)
    ventana._datos_recibidos(pd.DataFrame({"zona": ["Sur", "Norte"]}))
    assert ventana.estado.cget("text") == "Servidor detenido"
    assert "2" in ventana.estado_datos.cget("text")
    ventana.entry_filtro.insert(0, "Sur")
    ventana.aplicar_filtro()
    esperar(ventana, lambda: "Filtro aplicado" in ventana.estado_datos.cget("text"))
    assert ventana.estado.cget("text") == "Servidor detenido"
    assert len(ventana.df_actual) == 1
    ventana.limpiar_filtro()
    assert ventana.estado.cget("text") == "Servidor detenido"
    assert len(ventana.df_actual) == 2


def test_ordenar_encabezado_abarca_paginas_y_alterna(ventana):
    original = pd.DataFrame({"valor": [10, 2, None, 1], "zona": ["Sur"] * 4}, dtype=object)
    ventana.registros_por_pagina = 2
    ventana._datos_recibidos(original)
    # Ejecutar el comando real del encabezado, como un clic del usuario.
    ventana.tk.call(ventana.tabla.heading("valor", "command"))
    esperar(ventana, lambda: "ascendente" in ventana.estado_datos.cget("text"))
    assert ventana.df_actual["valor"].tolist() == [1, 2, 10, None]
    ventana.pagina_siguiente()
    assert ventana.tabla.item(ventana.tabla.get_children()[0], "values")[0] == "10"
    ventana.tk.call(ventana.tabla.heading("valor", "command"))
    esperar(ventana, lambda: "descendente" in ventana.estado_datos.cget("text"))
    assert ventana.df_actual["valor"].tolist() == [10, 2, 1, None]
    assert ventana.pagina_actual == 0
    assert original["valor"].tolist() == [10, 2, None, 1]


def test_ordenar_texto_filtrado_y_reiniciar_con_nuevas_columnas(ventana):
    ventana._datos_recibidos(pd.DataFrame({"nombre": ["Zeta", "Alfa", "Beta"], "zona": ["Sur", "Sur", "Norte"]}))
    ventana.entry_filtro.insert(0, "Sur")
    ventana.aplicar_filtro()
    esperar(ventana, lambda: "Filtro aplicado" in ventana.estado_datos.cget("text"))
    ventana.ordenar_columna("nombre")
    esperar(ventana, lambda: "ascendente" in ventana.estado_datos.cget("text"))
    assert ventana.df_actual["nombre"].tolist() == ["Alfa", "Zeta"]
    ventana.limpiar_filtro()
    assert ventana.df_actual["nombre"].tolist() == ["Zeta", "Alfa", "Beta"]
    assert ventana.tabla.heading("nombre", "text") == "nombre"
    ventana._datos_recibidos(pd.DataFrame({"nuevo": [2, 1]}))
    assert list(ventana.tabla["columns"]) == ["nuevo"]
    ventana.ordenar_columna("nuevo")
    esperar(ventana, lambda: "ascendente" in ventana.estado_datos.cget("text"))
    assert ventana.df_actual["nuevo"].tolist() == [1, 2]


def test_filtro_sin_resultados_conserva_encabezados(ventana):
    ventana._datos_recibidos(pd.DataFrame({"nombre": ["Sur"]}))
    ventana.entry_filtro.insert(0, "inexistente")
    ventana.aplicar_filtro()
    esperar(ventana, lambda: "Filtro aplicado" in ventana.estado_datos.cget("text"))
    assert ventana.tabla.heading("nombre", "text") == "nombre"
    assert not ventana.tabla.get_children()


def test_orden_mixto_y_error_se_muestran_solo_en_datos(ventana, monkeypatch):
    ventana._mostrar_estado("Servidor detenido", s.SUAVE)
    ventana._datos_recibidos(pd.DataFrame({"dato": [2, "texto", None]}, dtype=object))
    ventana.ordenar_columna("dato")
    esperar(ventana, lambda: "ascendente" in ventana.estado_datos.cget("text"))
    assert ventana.df_actual["dato"].tolist() == [2, "texto", None]
    def fallar(df, texto):
        raise ValueError("fallo de prueba")
    monkeypatch.setattr(s, "filtrar_dataframe", fallar)
    ventana.entry_filtro.insert(0, "x")
    ventana.aplicar_filtro()
    esperar(ventana, lambda: "fallo de prueba" in ventana.estado_datos.cget("text"))
    assert ventana.estado.cget("text") == "Servidor detenido"
    assert str(ventana.btn_filtrar.cget("state")) == "normal"


def test_orden_pendiente_no_reemplaza_dataset_nuevo(ventana, monkeypatch):
    inicio, continuar = threading.Event(), threading.Event()
    ordenar = pd.DataFrame.sort_values
    def lento(df, *args, **kwargs):
        inicio.set()
        assert continuar.wait(5)
        return ordenar(df, *args, **kwargs)
    monkeypatch.setattr(pd.DataFrame, "sort_values", lento)
    ventana._datos_recibidos(pd.DataFrame({"viejo": [2, 1]}))
    ventana.ordenar_columna("viejo")
    try:
        esperar(ventana, inicio.is_set)
        latido = []
        ventana.after(1, lambda: latido.append(True))
        esperar(ventana, lambda: bool(latido))
        nuevo = pd.DataFrame({"nuevo": [3]})
        ventana._datos_recibidos(nuevo)
    finally:
        continuar.set()
    esperar(ventana, lambda: not ventana._hilo_filtro.is_alive())
    esperar(ventana, lambda: ventana.cola.empty())
    assert ventana.df_actual is nuevo
    assert ventana.tabla.heading("nuevo", "text") == "nuevo"
