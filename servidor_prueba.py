"""
Script para recibir datos por sockets y mostrarlos en una tabla con filtros y ordenamiento.
Autor: Grupo 4 — Mamani, Perea, Pinedo, Vargas, Vergara.
Licencia: solo educacional.
Fecha creación: 16-10-2026.
Fecha modificación: 27-10-2026.
Modificaciones: separación de mensajes de conexión y datos, ajustes del filtro,
ordenamiento ascendente y descendente, e implementación de temporizador 
con envío inmediato de ACK para pruebas headless.

SERVIDOR OFICIAL (Grupo 4) - Dashboard de Ingesta Big Data.

Recibe el JSON masivo enviado por el Cliente, lo carga en memoria usando Pandas,
y lo visualiza mediante un sistema de paginación para evitar cuellos de botella.
Incluye filtros de vista dinámicos y respeta el "Contrato de Datos".
"""

import json
import os
import queue
import socket
import threading
import time
import logging
import math
import tkinter as tk
from tkinter import ttk
import pandas as pd

# ---------------------------------------------------------------------------
# Configuración y Estilos Visuales (Homologados con el Cliente)
# ---------------------------------------------------------------------------

HOST = os.environ.get("SERVIDOR_HOST", "127.0.0.1")
PUERTO = int(os.environ.get("SERVIDOR_PUERTO", "12345"))
MARCA_FIN = "<EOF>"

# Límites por conexión; 512 MiB permite la carga sintética de ocho columnas.
MAX_BYTES = int(os.environ.get("SERVIDOR_MAX_BYTES", str(512 * 1024 * 1024)))
TIEMPO_ESPERA = float(os.environ.get("SERVIDOR_TIMEOUT", "60"))
TIEMPO_TOTAL = float(os.environ.get("SERVIDOR_TIEMPO_TOTAL", "600"))
PASO_ESPERA = 0.2
if MAX_BYTES <= 5 or TIEMPO_ESPERA <= 0 or TIEMPO_TOTAL <= 0:
    raise ValueError("Configure límites positivos; SERVIDOR_MAX_BYTES debe superar cinco bytes.")


def recibir_todo(conexion, detener=None):
    """TCP es un flujo: esperar SHUT_WR y decodificar solo al finalizar."""
    conexion.settimeout(PASO_ESPERA)
    datos = bytearray()
    inicio = avance = time.monotonic()
    while True:
        ahora = time.monotonic()
        if detener is not None and detener.is_set():
            raise ValueError("Transferencia cancelada: servidor detenido.")
        if ahora - avance > TIEMPO_ESPERA or ahora - inicio > TIEMPO_TOTAL:
            raise TimeoutError("Tiempo de recepción agotado.")
        try:
            bloque = conexion.recv(1024 * 1024)
        except socket.timeout:
            continue
        if not bloque:
            return datos
        if len(datos) + len(bloque) > MAX_BYTES:
            raise ValueError("El cliente superó el tamaño máximo permitido.")
        datos.extend(bloque)
        avance = time.monotonic()


def decodificar(datos):
    marca = MARCA_FIN.encode("ascii")
    if not datos.endswith(marca):
        raise ValueError("Falta la marca <EOF>: el envío quedó incompleto.")
    # Solo se elimina el sufijo: las marcas dentro de valores son datos legítimos.
    del datos[-len(marca):]
    def constante_invalida(valor):
        raise ValueError(f"Constante JSON no válida: {valor}")
    def decimal_finito(valor):
        numero = float(valor)
        if not math.isfinite(numero):
            raise ValueError("Número fuera del rango finito admitido.")
        return numero
    def objeto(pares):
        resultado = {}
        for clave, valor in pares:
            if clave in resultado:
                raise ValueError(f"Columna JSON duplicada: {clave}")
            resultado[clave] = valor
        return resultado
    registros = json.loads(datos.decode("utf-8"), parse_constant=constante_invalida,
                           parse_float=decimal_finito,
                           object_pairs_hook=objeto)
    if not isinstance(registros, list) or not all(isinstance(r, dict) for r in registros):
        raise ValueError("El JSON debe ser una lista de objetos.")
    return registros


def dataframe_validado(registros):
    # object conserva los enteros Python exactos junto a None; no los convierte a float.
    return pd.DataFrame(registros, dtype=object)


def responder(conexion, respuesta):
    conexion.settimeout(1)
    conexion.sendall(json.dumps(respuesta, allow_nan=False).encode("utf-8"))


def atender(conexion, registrar=print):
    """Adaptador histórico sin GUI, con el mismo receptor y validaciones oficiales."""
    registros = []
    try:
        registros = decodificar(recibir_todo(conexion))
        dataframe_validado(registros)
        resultado = {"ok": True, "filas": len(registros), "detalle": "Recepción correcta."}
    except Exception as error:
        resultado = {"ok": False, "filas": 0, "detalle": str(error)}
    try:
        responder(conexion, resultado)
    except OSError:
        pass
    registrar(resultado["detalle"])
    return {**resultado, "registros": registros if resultado["ok"] else []}


def filtrar_dataframe(df, texto):
    """Búsqueda literal por columna; evita una copia textual completa del dataset."""
    mascara = pd.Series(False, index=df.index)
    for columna in df.columns:
        mascara |= df[columna].astype("string").str.contains(texto, case=False, regex=False, na=False)
    return df.loc[mascara]

FONDO, TARJETA, TEXTO, SUAVE = "#EEF2F5", "#FFFFFF", "#1F2A37", "#6B7785"
ACENTO, ACENTO_OSCURO, EXITO, ERROR, AVISO = "#0E7490", "#155E75", "#15803D", "#B91C1C", "#B45309"
FUENTE = "Segoe UI"

# Función de formato
def miles(numero):
    return f"{numero:,}".replace(",", ".")

class Tarjeta(ttk.Frame):
    """Panel blanco con título reutilizable."""
    def __init__(self, padre, titulo):
        super().__init__(padre, style="Tarjeta.TFrame", padding=14)
        if titulo:
            ttk.Label(self, text=titulo, style="Titulo.TLabel").pack(anchor="w", pady=(0, 8))

# ---------------------------------------------------------------------------
# Ventana Principal del Servidor
# ---------------------------------------------------------------------------

class ServidorBigData(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Servidor Dashboard · Grupo 4")
        self.geometry("1024x768")
        self.minsize(800, 600)
        self.configure(bg=FONDO)

        # Variables de Datos y Paginación
        self.df_recibido = None
        self.df_actual = None
        self.pagina_actual = 0
        self.registros_por_pagina = 1000
        self._orden = None  # (columna, ascendente); solo afecta la vista.

        # Variables de Red y Concurrencia
        self.servidor = None
        self.detener_escucha = threading.Event()
        self.cola = queue.Queue()
        self._cerrando = False
        self._after_cola = None
        self._hilo_escucha = None
        self._hilo_cliente = None
        self._hilo_filtro = None
        self._version_filtro = 0

        self._crear_estilos()
        self._crear_widgets()
        self.protocol("WM_DELETE_WINDOW", self._cerrar)
        self._after_cola = self.after(100, self._revisar_cola)

    def _crear_estilos(self):
        estilo = ttk.Style(self)
        estilo.theme_use("clam")
        estilo.configure(".", font=(FUENTE, 10), background=TARJETA, foreground=TEXTO)
        estilo.configure("Tarjeta.TFrame", background=TARJETA)
        estilo.configure("Fondo.TFrame", background=FONDO)
        estilo.configure("Titulo.TLabel", font=(FUENTE, 12, "bold"), background=TARJETA)
        estilo.configure("Suave.TLabel", foreground=SUAVE, background=TARJETA)
        estilo.configure("TButton", padding=(12, 6), background="#E3E8ED", borderwidth=0, font=(FUENTE, 10, "bold"))
        estilo.map("TButton", background=[("active", "#D3DBE2"), ("disabled", "#F1F4F6")])
        estilo.configure("Acento.TButton", background=ACENTO, foreground="white")
        estilo.map("Acento.TButton", background=[("active", ACENTO_OSCURO), ("disabled", "#A5C8D2")], foreground=[("disabled", "#F4F8F9")])
        estilo.configure("Alto.TButton", background=ERROR, foreground="white")
        estilo.map("Alto.TButton", background=[("active", "#991B1B"), ("disabled", "#E5B4B4")], foreground=[("disabled", "#FBF1F1")])
        estilo.configure("Treeview", rowheight=24, fieldbackground=TARJETA, borderwidth=0)
        estilo.configure("Treeview.Heading", font=(FUENTE, 9, "bold"), background="#E3E8ED")

    def _crear_widgets(self):
        # Encabezado
        encabezado = ttk.Frame(self, style="Fondo.TFrame", padding=(20, 16, 20, 4))
        encabezado.pack(fill="x")
        tk.Label(encabezado, text="Dashboard de Ingesta de Datos", bg=FONDO, fg=TEXTO, font=(FUENTE, 20, "bold")).pack(anchor="w")
        tk.Label(encabezado, text="Recibe, procesa y visualiza grandes volúmenes de datos enviados por los clientes.", bg=FONDO, fg=SUAVE, font=(FUENTE, 10)).pack(anchor="w")

        cuerpo = ttk.Frame(self, style="Fondo.TFrame", padding=(20, 8, 20, 16))
        cuerpo.pack(fill="both", expand=True)

        # SECCIÓN 1: Controles de Red y Estado
        panel_red = Tarjeta(cuerpo, "Control del Servidor")
        panel_red.pack(fill="x", pady=(0, 12))
        
        cont_red = ttk.Frame(panel_red, style="Tarjeta.TFrame")
        cont_red.pack(fill="x")
        
        self.var_ip, self.var_puerto = tk.StringVar(value=HOST), tk.StringVar(value=str(PUERTO))
        ttk.Label(cont_red, text="IP", style="Suave.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Entry(cont_red, textvariable=self.var_ip, width=16).grid(row=0, column=1, padx=(6, 14))
        ttk.Label(cont_red, text="Puerto", style="Suave.TLabel").grid(row=0, column=2, sticky="w")
        ttk.Entry(cont_red, textvariable=self.var_puerto, width=7).grid(row=0, column=3, padx=(6, 14))
        
        self.boton_red = ttk.Button(cont_red, text="Iniciar escucha", style="Acento.TButton", command=self.alternar_servidor)
        self.boton_red.grid(row=0, column=4, sticky="e")
        cont_red.columnconfigure(4, weight=1)
        
        self.estado = tk.Label(panel_red, text="● Servidor detenido", bg=TARJETA, fg=SUAVE, font=(FUENTE, 10, "bold"), anchor="w")
        self.estado.pack(anchor="w", pady=(10, 0))

        # SECCIÓN 2: Filtros de Vista
        panel_filtro = Tarjeta(cuerpo, "Filtro de búsqueda")
        panel_filtro.pack(fill="x", pady=(0, 12))
        controles = ttk.Frame(panel_filtro, style="Tarjeta.TFrame")
        controles.pack(fill="x")
        
        ttk.Label(controles, text="Buscar texto:", style="Suave.TLabel").pack(side="left")
        self.entry_filtro = ttk.Entry(controles, width=25)
        self.entry_filtro.pack(side="left", padx=10)
        
        self.btn_filtrar = ttk.Button(controles, text="Aplicar", command=self.aplicar_filtro, state="disabled")
        self.btn_filtrar.pack(side="left", padx=5)
        self.btn_limpiar = ttk.Button(controles, text="Limpiar", command=self.limpiar_filtro, state="disabled")
        self.btn_limpiar.pack(side="left", padx=5)
        self.estado_datos = tk.Label(controles, text="Esperando datos...", bg=TARJETA,
                                     fg=SUAVE, anchor="w", justify="left", font=(FUENTE, 10, "bold"))
        self.estado_datos.pack(side="left", fill="x", expand=True, padx=(10, 0))
        self.estado_datos.bind("<Configure>", lambda e: self.estado_datos.configure(wraplength=max(1, e.width)))

        # SECCIÓN 3: Paginación
        panel_pag = ttk.Frame(cuerpo, style="Fondo.TFrame")
        panel_pag.pack(fill="x", pady=(0, 5))
        
        self.btn_anterior = ttk.Button(panel_pag, text="<< Anterior", command=self.pagina_anterior, state="disabled")
        self.btn_anterior.pack(side="left")
        
        self.lbl_pagina = tk.Label(panel_pag, text="Esperando datos...", bg=FONDO, fg=TEXTO, font=(FUENTE, 10, "bold"))
        self.lbl_pagina.pack(side="left", padx=15)
        
        self.btn_siguiente = ttk.Button(panel_pag, text="Siguiente >>", command=self.pagina_siguiente, state="disabled")
        self.btn_siguiente.pack(side="left")

        # SECCIÓN 4: Dashboard (Treeview)
        panel_tabla = Tarjeta(cuerpo, "")
        panel_tabla.pack(fill="both", expand=True)
        
        self.tabla = ttk.Treeview(panel_tabla, show="headings")
        scroll_y = ttk.Scrollbar(panel_tabla, orient="vertical", command=self.tabla.yview)
        scroll_x = ttk.Scrollbar(panel_tabla, orient="horizontal", command=self.tabla.xview)
        self.tabla.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        
        scroll_y.pack(side="right", fill="y")
        scroll_x.pack(side="bottom", fill="x")
        self.tabla.pack(side="left", fill="both", expand=True)

    # -----------------------------------------------------------------------
    # LÓGICA DE RED (Sockets)
    # -----------------------------------------------------------------------

    def _mostrar_estado(self, texto, color):
        self.estado.configure(text=texto, fg=color)

    def _mostrar_estado_datos(self, texto, color):
        self.estado_datos.configure(text=texto, fg=color)

    def alternar_servidor(self):
        if self.servidor is None:
            self.iniciar()
        else:
            self.detener()

    def iniciar(self):
        if self.servidor is not None or self._cerrando:
            return
        host, texto_puerto = self.var_ip.get().strip(), self.var_puerto.get().strip()
        if not host or not texto_puerto.isdigit() or not 0 < int(texto_puerto) < 65536:
            self._mostrar_estado("● Revise la IP y el puerto (1-65535)", ERROR)
            return
            
        try:
            serv_socket = socket.create_server((host, int(texto_puerto)))
        except OSError as error:
            motivo = "el puerto ya está en uso" if error.errno in (10048, 98) else str(error)
            self._mostrar_estado(f"● Error al abrir puerto {texto_puerto}: {motivo}", ERROR)
            return
            
        serv_socket.settimeout(0.5)
        self.servidor = serv_socket
        self.detener_escucha.clear()
        
        self._hilo_escucha = threading.Thread(target=self._bucle_escucha, args=(serv_socket,), daemon=True)
        self._hilo_escucha.start()
        
        self.boton_red.configure(text="Detener escucha", style="Alto.TButton")
        self._mostrar_estado(f"● Escuchando en {host}:{texto_puerto}", EXITO)

    def detener(self):
        if self.servidor is not None:
            self.detener_escucha.set()
            self.boton_red.configure(state="disabled")
            self._mostrar_estado("● Deteniendo servidor...", AVISO)

    def _al_detenerse(self):
        self.servidor = None
        self.boton_red.configure(text="Iniciar escucha", style="Acento.TButton", state="normal")
        self._mostrar_estado("● Servidor detenido", SUAVE)

    def _bucle_escucha(self, servidor):
        try:
            while not self.detener_escucha.is_set():
                try:
                    conexion, direccion = servidor.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                if self.detener_escucha.is_set() or (
                    self._hilo_cliente is not None and self._hilo_cliente.is_alive()
                ):
                    try:
                        responder(conexion, {"ok": False, "filas": 0,
                                            "detalle": "Servidor ocupado o deteniéndose; reintente."})
                    except OSError:
                        pass
                    finally:
                        conexion.close()
                    continue
                self.cola.put(lambda ip=direccion[0]: self._mostrar_estado(
                    f"● Recibiendo paquete de {ip}...", AVISO))
                self._hilo_cliente = threading.Thread(
                    target=self._atender_cliente, args=(conexion,), daemon=True)
                self._hilo_cliente.start()
        finally:
            servidor.close()
            if self._hilo_cliente is not None:
                self._hilo_cliente.join()
            self.cola.put(self._al_detenerse)

    # Propósito: recibir, validar y aplicar los datos en la interfaz ANTES de responder el ACK.
    # Así "ok: true" significa "recibido y aceptado", no solo "recibido".
    def _atender_cliente(self, conexion):
        inicio = time.time()
        try:
            registros = decodificar(recibir_todo(conexion, self.detener_escucha))
            df_nuevo = dataframe_validado(registros)
            del registros
            demora = time.time() - inicio   # red + validación, se muestra en pantalla
            aceptado = threading.Event()
            cancelado = threading.Event()
            decision = threading.Lock()
            resultado = {}
            # La decisión, su aplicación y la señal se realizan en el hilo Tk.
            def aceptar():
                with decision:
                    anterior = self.df_recibido, self.df_actual, self.pagina_actual
                    try:
                        if cancelado.is_set() or self.detener_escucha.is_set() or self._cerrando:
                            raise ValueError("Transferencia cancelada: servidor detenido o aceptación vencida.")
                        self._datos_recibidos(df_nuevo, demora)
                        self._mostrar_estado("● Recepción completada. Servidor a la escucha.", EXITO)
                        resultado.update(ok=True, filas=len(df_nuevo))
                    except Exception as error:
                        self.df_recibido, self.df_actual, self.pagina_actual = anterior
                        resultado.update(ok=False, filas=0, detalle=str(error))
                    finally:
                        aceptado.set()
            self.cola.put(aceptar)
            limite = time.monotonic() + TIEMPO_ESPERA
            while not aceptado.wait(PASO_ESPERA):
                if self.detener_escucha.is_set() or time.monotonic() > limite:
                    with decision:
                        if aceptado.is_set():
                            break
                        cancelado.set()
                    raise ValueError("Transferencia cancelada o tiempo de aceptación agotado.")
            responder(conexion, resultado)
        except Exception as error:
            mensaje = str(error)
            try:
                responder(conexion, {"ok": False, "filas": 0, "detalle": mensaje})
            except OSError:
                pass
            self.cola.put(lambda texto=mensaje: self._mostrar_estado(
                f"● Error en transferencia: {texto}", ERROR))
        finally:
            conexion.close()

    def _datos_recibidos(self, df_nuevo, demora=0.0):
        self._version_filtro += 1
        self._orden = None
        self.entry_filtro.delete(0, tk.END)
        self.df_recibido = df_nuevo
        self.df_actual = self.df_recibido
        self.pagina_actual = 0
        
        self.cargar_datos_en_tabla()
        
        self._mostrar_estado_datos(
            f"● Datos recibidos: {miles(len(self.df_recibido))} registros. (Demora: {demora:.1f} s)", EXITO
        )
        self.btn_filtrar.configure(state="normal")
        self.btn_limpiar.configure(state="normal")

    # -----------------------------------------------------------------------
    # LÓGICA DE INTERFAZ, PAGINACIÓN Y FILTROS
    # -----------------------------------------------------------------------

    def cargar_datos_en_tabla(self):
        self.tabla.delete(*self.tabla.get_children())
        columnas = [] if self.df_actual is None else list(self.df_actual.columns)
        self.tabla["columns"] = columnas
        for col in columnas:
            flecha = ""
            if self._orden and self._orden[0] == col:
                flecha = " ▲" if self._orden[1] else " ▼"
            self.tabla.heading(col, text=col + flecha, command=lambda columna=col: self.ordenar_columna(columna))
            self.tabla.column(col, width=120, minwidth=80, anchor="center")
        if self.df_actual is None or len(self.df_actual) == 0:
            self.pagina_actual = 0
            self.btn_anterior.config(state="disabled")
            self.btn_siguiente.config(state="disabled")
            self.lbl_pagina.config(text="Sin resultados. · 0 registros")
            return

        # Calcular Slices para Paginación
        inicio = self.pagina_actual * self.registros_por_pagina
        fin = inicio + self.registros_por_pagina
        df_pagina = self.df_actual.iloc[inicio:fin]
        
        # Inserción rápida convirtiendo a listas
        filas_valores = df_pagina.astype(object).where(df_pagina.notna(), "—").values.tolist()
        for fila in filas_valores:
            self.tabla.insert("", "end", values=fila)
            
        # Actualizar textos y botones
        total_paginas = max(1, (len(self.df_actual) - 1) // self.registros_por_pagina + 1)
        mostrando_fin = min(fin, len(self.df_actual))
        self.lbl_pagina.config(text=f"Página {self.pagina_actual + 1} de {total_paginas}  ·  Mostrando {miles(inicio + 1)} al {miles(mostrando_fin)}")
        
        self.btn_anterior.config(state="normal" if self.pagina_actual > 0 else "disabled")
        self.btn_siguiente.config(state="normal" if fin < len(self.df_actual) else "disabled")

    def pagina_siguiente(self):
        if self.df_actual is not None and (self.pagina_actual + 1) * self.registros_por_pagina < len(self.df_actual):
            self.pagina_actual += 1
            self.cargar_datos_en_tabla()

    def pagina_anterior(self):
        if self.pagina_actual > 0:
            self.pagina_actual -= 1
            self.cargar_datos_en_tabla()

    def aplicar_filtro(self):
        texto = self.entry_filtro.get().strip()
        if self.df_recibido is None or (self._hilo_filtro and self._hilo_filtro.is_alive()):
            return
        if not texto:
            self.limpiar_filtro()
            return
        original = self.df_recibido
        self._calcular_vista(lambda: filtrar_dataframe(original, texto),
                             "Filtrando datos...", "Filtro aplicado: {filas} coincidencias.")

    def ordenar_columna(self, columna):
        if self.df_actual is None or columna not in self.df_actual.columns:
            return
        ascendente = not self._orden[1] if self._orden and self._orden[0] == columna else True
        original = self.df_actual
        def ordenar():
            try:
                return original.sort_values(columna, ascending=ascendente, kind="stable", na_position="last")
            except TypeError:
                # Una columna con tipos mezclados se compara como texto; los vacíos quedan al final.
                return original.sort_values(columna, ascending=ascendente, kind="stable",
                                            na_position="last", key=lambda serie: serie.astype("string"))
        sentido = "ascendente" if ascendente else "descendente"
        self._calcular_vista(ordenar, "Ordenando datos...",
                             f"Orden {sentido}: {columna}.", (columna, ascendente))

    def _calcular_vista(self, calcular, mensaje, resultado, orden=None):
        """Filtro y orden comparten un trabajador; solo Tk aplica el resultado vigente."""
        if self._hilo_filtro and self._hilo_filtro.is_alive():
            return
        self._version_filtro += 1
        version = self._version_filtro
        self._mostrar_estado_datos(mensaje, AVISO)
        self.btn_filtrar.configure(state="disabled")
        def trabajo():
            try:
                nuevo = calcular()
                def aplicar():
                    if version != self._version_filtro:
                        return
                    self.df_actual = nuevo
                    self._orden = orden
                    self.pagina_actual = 0
                    self.cargar_datos_en_tabla()
                    self._mostrar_estado_datos(resultado.replace("{filas}", miles(len(nuevo))), EXITO)
                self.cola.put(aplicar)
            except Exception as error:
                def fallar(texto=str(error)):
                    if version == self._version_filtro:
                        self._mostrar_estado_datos(f"Error al actualizar la vista: {texto}", ERROR)
                self.cola.put(fallar)
            finally:
                self.cola.put(lambda: self.btn_filtrar.configure(state="normal"))
        self._hilo_filtro = threading.Thread(target=trabajo, daemon=True)
        self._hilo_filtro.start()

    def limpiar_filtro(self):
        self._orden = None
        self._version_filtro += 1
        if self.df_recibido is None:
            return
        self.entry_filtro.delete(0, tk.END)
        self.df_actual = self.df_recibido
        self.pagina_actual = 0
        self.cargar_datos_en_tabla()
        self._mostrar_estado_datos(f"● Mostrando dataset completo: {miles(len(self.df_actual))} registros.", EXITO)

    def _revisar_cola(self):
        try:
            # Un lote acotado evita monopolizar el bucle de eventos.
            for _ in range(50):
                try:
                    tarea = self.cola.get_nowait()
                except queue.Empty:
                    break
                try:
                    tarea()
                except Exception:
                    logging.exception("Error en tarea del servidor")
        finally:
            if not self._cerrando:
                self._after_cola = self.after(100, self._revisar_cola)

    def _cerrar(self):
        if self._cerrando:
            return
        self._cerrando = True
        self.detener_escucha.set()
        self._version_filtro += 1
        if self._after_cola is not None:
            self.after_cancel(self._after_cola)
        self._esperar_cierre()

    def _esperar_cierre(self):
        # No bloquear Tk mientras los trabajadores observan la cancelación.
        hilos = (self._hilo_escucha, self._hilo_cliente, self._hilo_filtro)
        if any(h is not None and h.is_alive() for h in hilos):
            self.after(50, self._esperar_cierre)
        else:
            while not self.cola.empty():
                self.cola.get_nowait()
            self.destroy()


class ServidorPrueba(ServidorBigData):
    """Adaptador de los nombres históricos usados por las pruebas de Grupo 3."""
    def __init__(self):
        super().__init__()
        self.boton = self.boton_red
        self.registro = tk.Text(self, height=3, state="disabled")
        self.registro.pack(fill="x")

    def alternar(self):
        self.alternar_servidor()

    def _atender_cliente(self, conexion):
        inicio = time.time()  # ⏱️ 1. Iniciamos cronómetro
        try:
            registros = decodificar(recibir_todo(conexion, self.detener_escucha))
            df_nuevo = dataframe_validado(registros)
            del registros
            
            demora = time.time() - inicio  # ⏱️ 2. Calculamos tiempo de red y procesamiento
            
            aceptado = threading.Event()
            cancelado = threading.Event()
            decision = threading.Lock()
            resultado = {}
            
            # La decisión, su aplicación y la señal se realizan en el hilo Tk.
            def aceptar():
                with decision:
                    anterior = self.df_recibido, self.df_actual, self.pagina_actual
                    try:
                        if cancelado.is_set() or self.detener_escucha.is_set() or self._cerrando:
                            raise ValueError("Transferencia cancelada: servidor detenido o aceptación vencida.")
                        
                        # 🎨 3. Pasamos los datos y el tiempo a la interfaz gráfica
                        self._datos_recibidos(df_nuevo, demora)
                        self._mostrar_estado("● Recepción completada. Servidor a la escucha.", EXITO)
                        
                        resultado.update(ok=True, filas=len(df_nuevo))
                    except Exception as error:
                        self.df_recibido, self.df_actual, self.pagina_actual = anterior
                        resultado.update(ok=False, filas=0, detalle=str(error))
                    finally:
                        aceptado.set()
            
            self.cola.put(aceptar)
            limite = time.monotonic() + TIEMPO_ESPERA
            
            # 🔒 4. Esperamos estrictamente a que la GUI avise que terminó antes del ACK
            while not aceptado.wait(PASO_ESPERA):
                if self.detener_escucha.is_set() or time.monotonic() > limite:
                    with decision:
                        if aceptado.is_set():
                            break
                        cancelado.set()
                    raise ValueError("Transferencia cancelada o tiempo de aceptación agotado.")
            
            # ✅ 5. Enviamos el ACK final
            responder(conexion, resultado)
            
        except Exception as error:
            mensaje = str(error)
            try:
                responder(conexion, {"ok": False, "filas": 0, "detalle": mensaje})
            except OSError:
                pass
            self.cola.put(lambda texto=mensaje: self._mostrar_estado(
                f"● Error en transferencia: {texto}", ERROR))
        finally:
            conexion.close()

    def _datos_recibidos(self, df_nuevo, demora=0.0):
        super()._datos_recibidos(df_nuevo, demora)
        self.registro.configure(state="normal")
        self.registro.insert("end", f"{len(df_nuevo)} filas recibidas\n")
        self.registro.configure(state="disabled")


if __name__ == "__main__":
    ServidorBigData().mainloop()