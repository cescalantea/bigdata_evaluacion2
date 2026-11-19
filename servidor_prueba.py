"""
Script para recibir datos por sockets y mostrarlos en una tabla con filtros,
ordenamiento, dashboard estadístico (Matplotlib) y persistencia en MongoDB.
Autor: Grupo 4 — Mamani, Perea, Pinedo, Vargas, Vergara.
Licencia: solo educacional.
Fecha creación: 16-10-2026.
Fecha modificación: 19-11-2026.
Modificaciones: integración de doble puerto (CSV y sensores), persistencia en
MongoDB Atlas y pestaña de Dashboard Estadístico descriptivo con Matplotlib.

SERVIDOR OFICIAL (Grupo 4) - Dashboard de Ingesta Big Data.

Recibe el JSON masivo enviado por el Cliente, lo carga en memoria usando Pandas,
lo visualiza mediante paginación, genera estadísticas descriptivas con gráficos
y persiste los datos recibidos respetando el "Contrato de Datos".
"""

import json
import os
import queue
import socket
import threading
import time
import logging
import math
import uuid
from datetime import datetime, timezone
import tkinter as tk
from tkinter import ttk
import pandas as pd

try:
    from dotenv import load_dotenv
except ImportError:  # Permite ejecutar la demo sin configurar Atlas.
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

try:
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    MATPLOTLIB_DISPONIBLE = True
except ImportError:
    MATPLOTLIB_DISPONIBLE = False

# ---------------------------------------------------------------------------
# Configuración y Estilos Visuales (Homologados con el Cliente)
# ---------------------------------------------------------------------------

HOST = os.environ.get("SERVIDOR_HOST", "127.0.0.1")
PUERTO = int(os.environ.get("SERVIDOR_PUERTO", "12345"))
PUERTO_SENSORES = int(os.environ.get("SERVIDOR_PUERTO_SENSORES", "12346"))
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

    registros = json.loads(
        datos.decode("utf-8"),
        parse_constant=constante_invalida,
        parse_float=decimal_finito,
        object_pairs_hook=objeto,
    )
    if not isinstance(registros, list) or not all(isinstance(r, dict) for r in registros):
        raise ValueError("El JSON debe ser una lista de objetos.")
    return registros


def dataframe_validado(registros):
    # object conserva los enteros Python exactos junto a None; no los convierte a float.
    return pd.DataFrame(registros, dtype=object)


def responder(conexion, respuesta):
    conexion.settimeout(1)
    conexion.sendall(json.dumps(respuesta, allow_nan=False).encode("utf-8"))


def responder_linea(conexion, respuesta):
    """ACK para el protocolo tipado: un objeto JSON por linea."""
    conexion.settimeout(1)
    conexion.sendall(json.dumps(respuesta, allow_nan=False).encode("utf-8") + b"\n")


def recibir_sensores(conexion, on_sensor, detener=None):
    """Recibe JSON Lines de sensores en una conexion dedicada."""
    conexion.settimeout(PASO_ESPERA)
    buffer = bytearray()
    inicio = avance = time.monotonic()
    cantidad = 0

    def procesar(linea):
        nonlocal cantidad
        try:
            mensaje = json.loads(linea.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as error:
            raise ValueError(f"Mensaje SENSOR invalido: {error}") from error
        if not isinstance(mensaje, dict) or mensaje.get("type") != "SENSOR":
            raise ValueError("El puerto de sensores solo acepta mensajes type=SENSOR.")
        on_sensor(mensaje, conexion)
        cantidad += 1

    while True:
        ahora = time.monotonic()
        if detener is not None and detener.is_set():
            raise ValueError("Recepcion de sensores cancelada: servidor detenido.")
        if ahora - avance > TIEMPO_ESPERA or ahora - inicio > TIEMPO_TOTAL:
            raise TimeoutError("Tiempo de recepcion de sensores agotado.")
        try:
            bloque = conexion.recv(64 * 1024)
        except socket.timeout:
            continue
        if not bloque:
            break
        avance = time.monotonic()
        buffer.extend(bloque)
        if len(buffer) > MAX_BYTES:
            raise ValueError("El canal de sensores supero el tamaño maximo permitido.")
        while b"\n" in buffer:
            linea, _, resto = buffer.partition(b"\n")
            buffer = bytearray(resto)
            if linea.strip():
                procesar(linea.strip())

    if buffer.strip():
        procesar(buffer.strip())
    return cantidad


def recibir_contrato(conexion, detener=None, on_sensor=None):
    """Recibe el contrato antiguo o el nuevo contrato tipado.

    Compatibilidad historica:
        ``[{...},{...}]<EOF>`` se interpreta como un CSV completo.

    Contrato nuevo:
        Cada linea es un JSON con ``type``. Se aceptan ``SENSOR``,
        ``CSV_START``, ``CSV_DATA`` y ``CSV_END``. Como TCP es un flujo,
        las lineas se reconstruyen aunque lleguen fragmentadas.
    """
    conexion.settimeout(PASO_ESPERA)
    inicio = avance = time.monotonic()
    buffer = bytearray()
    legado = bytearray()
    modo = None
    registros = []
    csv_iniciado = False
    csv_finalizado = False
    sensores = 0

    def procesar_linea(linea):
        nonlocal csv_iniciado, csv_finalizado, sensores
        try:
            mensaje = json.loads(
                linea.decode("utf-8"),
                parse_constant=lambda valor: (_ for _ in ()).throw(
                    ValueError(f"Constante JSON no valida: {valor}")
                ),
            )
        except (UnicodeError, json.JSONDecodeError, ValueError) as error:
            raise ValueError(f"Mensaje tipado invalido: {error}") from error
        if not isinstance(mensaje, dict):
            raise ValueError("Cada mensaje tipado debe ser un objeto JSON.")
        tipo = mensaje.get("type")
        if tipo == "SENSOR":
            if not {"temp", "hum", "gas"}.issubset(mensaje):
                raise ValueError("El mensaje SENSOR requiere temp, hum y gas.")
            if on_sensor is not None:
                on_sensor(mensaje, conexion)
            sensores += 1
            return
        if tipo == "CSV_START":
            if csv_iniciado:
                raise ValueError("El CSV ya fue iniciado en esta conexion.")
            csv_iniciado = True
            return
        if tipo == "CSV_DATA":
            if not csv_iniciado or csv_finalizado:
                raise ValueError("CSV_DATA debe estar entre CSV_START y CSV_END.")
            filas = mensaje.get("rows")
            if not isinstance(filas, list) or not all(isinstance(fila, dict) for fila in filas):
                raise ValueError("CSV_DATA.rows debe ser una lista de objetos.")
            registros.extend(filas)
            return
        if tipo == "CSV_END":
            if not csv_iniciado:
                raise ValueError("CSV_END recibido sin CSV_START.")
            if mensaje.get("status", "OK") != "OK":
                raise ValueError("El cliente marco la transferencia CSV como fallida.")
            csv_finalizado = True
            return
        raise ValueError(f"Tipo de mensaje no reconocido: {tipo!r}.")

    while True:
        ahora = time.monotonic()
        if detener is not None and detener.is_set():
            raise ValueError("Transferencia cancelada: servidor detenido.")
        if ahora - avance > TIEMPO_ESPERA or ahora - inicio > TIEMPO_TOTAL:
            raise TimeoutError("Tiempo de recepcion agotado.")
        try:
            bloque = conexion.recv(1024 * 1024)
        except socket.timeout:
            continue
        if not bloque:
            break
        avance = time.monotonic()
        if modo is None:
            buffer.extend(bloque)
            prefijo = bytes(buffer).lstrip()
            if not prefijo:
                continue
            if prefijo.startswith(b"["):
                modo = "LEGACY"
                legado.extend(buffer)
                buffer.clear()
            elif prefijo.startswith(b"{"):
                modo = "TIPADO"
            else:
                raise ValueError("El mensaje debe comenzar con JSON de lista u objeto.")
        elif modo == "LEGACY":
            legado.extend(bloque)
        else:
            buffer.extend(bloque)

        if modo == "LEGACY":
            if len(legado) > MAX_BYTES:
                raise ValueError("El cliente supero el tamaño maximo permitido.")
            continue

        while b"\n" in buffer:
            linea, _, resto = buffer.partition(b"\n")
            buffer = bytearray(resto)
            if linea.strip():
                if len(linea) > MAX_BYTES:
                    raise ValueError("El mensaje tipado supero el tamaño maximo permitido.")
                procesar_linea(linea.strip())
        if len(buffer) > MAX_BYTES:
            raise ValueError("El mensaje tipado supero el tamaño maximo permitido.")
        if csv_finalizado:
            break

    if modo == "LEGACY":
        return {"modo": modo, "registros": decodificar(legado), "sensores": 0, "es_csv": True}
    if modo != "TIPADO":
        raise ValueError("No se recibio ningun mensaje.")
    if buffer.strip():
        procesar_linea(buffer.strip())
    if csv_iniciado and not csv_finalizado:
        raise ValueError("Falta CSV_END: el envio tipado quedo incompleto.")
    return {"modo": modo, "registros": registros, "sensores": sensores, "es_csv": csv_iniciado}


def atender(conexion, registrar=print):
    """Adaptador histórico sin GUI, con el mismo receptor y validaciones oficiales.

    Su ACK significa disponible en el resultado retornado; la GUI oficial espera
    además la aplicación del dataset en el hilo principal.
    """
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


class MongoRepositorio:
    """Persistencia MongoDB Atlas por lotes, sin credenciales en el codigo."""

    def __init__(self):
        self.uri = os.environ.get("MONGODB_URI", "").strip()
        self.nombre_bd = os.environ.get("MONGODB_DB", "bigdata_eval3").strip()
        self.coleccion_csv = os.environ.get("MONGODB_COLLECTION_CSV", "datos_csv").strip()
        self.coleccion_sensores = os.environ.get("MONGODB_COLLECTION_SENSORES", "sensores").strip()
        self.tamano_lote = int(os.environ.get("MONGODB_BATCH_SIZE", "1000"))
        self.timeout_ms = int(os.environ.get("MONGODB_TIMEOUT_MS", "5000"))
        if self.tamano_lote <= 0 or self.timeout_ms <= 0:
            raise ValueError("MONGODB_BATCH_SIZE y MONGODB_TIMEOUT_MS deben ser positivos.")
        self._cliente = None
        self._db = None
        self._conexion_lock = threading.Lock()

    @property
    def habilitado(self):
        """Indica si se configuro una URI de MongoDB."""
        return bool(self.uri)

    def conectar(self):
        """Abre la conexion y comprueba que Atlas responda."""
        if not self.habilitado:
            return False
        with self._conexion_lock:
            if self._cliente is not None:
                return True
            try:
                from pymongo import MongoClient
                self._cliente = MongoClient(self.uri, serverSelectionTimeoutMS=self.timeout_ms)
                self._cliente.admin.command("ping")
                self._db = self._cliente[self.nombre_bd]
                return True
            except ImportError as error:
                raise RuntimeError(
                    "Falta pymongo. Ejecute: python -m pip install -r requirements.txt"
                ) from error
            except Exception:
                if self._cliente is not None:
                    self._cliente.close()
                self._cliente = None
                self._db = None
                raise

    @staticmethod
    def _valor_mongo(valor):
        """Convierte valores de Pandas/Numpy a tipos aceptados por MongoDB."""
        if valor is None:
            return None
        if hasattr(valor, "item"):
            try:
                valor = valor.item()
            except (ValueError, TypeError):
                pass
        if isinstance(valor, pd.Timestamp):
            return valor.to_pydatetime()
        try:
            faltante = pd.isna(valor)
            if isinstance(faltante, bool) and faltante:
                return None
        except (TypeError, ValueError):
            pass
        return valor

    def guardar_dataframe(self, dataframe, transferencia_id=None):
        """Guarda filas del CSV con insert_many, sin un insert por fila."""
        if not self.habilitado:
            return {"habilitado": False, "insertados": 0, "transferencia_id": None}
        if dataframe is None:
            raise ValueError("No se puede guardar un DataFrame inexistente en MongoDB.")
        self.conectar()
        transferencia_id = transferencia_id or uuid.uuid4().hex
        coleccion = self._db[self.coleccion_csv]
        insertados = 0
        documentos = []
        recibido_en = datetime.now(timezone.utc)
        for numero_fila, fila in enumerate(dataframe.to_dict("records"), start=1):
            documentos.append({
                "transfer_id": transferencia_id,
                "row_number": numero_fila,
                "data": {str(clave): self._valor_mongo(valor) for clave, valor in fila.items()},
                "received_at": recibido_en,
            })
            if len(documentos) >= self.tamano_lote:
                coleccion.insert_many(documentos, ordered=False)
                insertados += len(documentos)
                documentos.clear()
        if documentos:
            coleccion.insert_many(documentos, ordered=False)
            insertados += len(documentos)
        return {"habilitado": True, "insertados": insertados, "transferencia_id": transferencia_id}

    def guardar_sensor(self, temperatura, humedad=None, gas=None, fecha_hora=None):
        """Guarda una lectura IoT; queda listo para el protocolo SENSOR."""
        if not self.habilitado:
            return {"habilitado": False, "insertados": 0}
        self.conectar()
        documento = {
            "type": "SENSOR",
            "fecha_hora": fecha_hora or datetime.now(timezone.utc),
            "temperatura": self._valor_mongo(temperatura),
            "humedad": self._valor_mongo(humedad),
            "gas": self._valor_mongo(gas),
        }
        self._db[self.coleccion_sensores].insert_one(documento)
        return {"habilitado": True, "insertados": 1}

    def cerrar(self):
        """Cierra el cliente MongoDB al salir de la aplicacion."""
        if self._cliente is not None:
            self._cliente.close()
            self._cliente = None
            self._db = None


FONDO, TARJETA, TEXTO, SUAVE = "#EEF2F5", "#FFFFFF", "#1F2A37", "#6B7785"
ACENTO, ACENTO_OSCURO, EXITO, ERROR, AVISO = "#0E7490", "#155E75", "#15803D", "#B91C1C", "#B45309"
FUENTE = "Segoe UI"


def miles(numero):
    return f"{numero:,}".replace(",", ".")


class Tarjeta(ttk.Frame):
    """Panel blanco con título reutilizable."""

    def __init__(self, padre, titulo):
        super().__init__(padre, style="Tarjeta.TFrame", padding=14)
        if titulo:
            ttk.Label(self, text=titulo, style="Titulo.TLabel").pack(anchor="w", pady=(0, 8))


class DashboardEstadistico(ttk.Frame):
    """Panel de solo consulta con estadística descriptiva y gráficos del CSV."""

    def __init__(self, padre):
        super().__init__(padre, style="Fondo.TFrame", padding=(0, 8, 0, 0))
        self._canvas = None
        self._crear_interfaz()

    def _crear_interfaz(self):
        # 1. Tarjeta superior: KPIs generales y estado
        panel_kpi = Tarjeta(self, "Resumen Estadístico del CSV (Solo consulta)")
        panel_kpi.pack(fill="x", pady=(0, 10))

        self.lbl_resumen = tk.Label(
            panel_kpi,
            text="Esperando la recepción de un archivo CSV para calcular estadísticas...",
            bg=TARJETA, fg=SUAVE, font=(FUENTE, 10, "bold"), anchor="w"
        )
        self.lbl_resumen.pack(fill="x")

        # 2. Tabla de medidas descriptivas por columna numérica
        panel_stats = Tarjeta(self, "Medidas Básicas por Variable Numérica")
        panel_stats.pack(fill="x", pady=(0, 10))

        cols = ("Columna", "Válidos", "Promedio", "Mínimo", "Máximo", "Desv. Est.")
        self.tabla_stats = ttk.Treeview(panel_stats, columns=cols, show="headings", height=4)
        for col in cols:
            self.tabla_stats.heading(col, text=col)
            self.tabla_stats.column(col, width=130, anchor="center")
        self.tabla_stats.pack(fill="x")

        # 3. Contenedor para los 2 gráficos de Matplotlib
        self.panel_graficos = Tarjeta(self, "Visualización de Distribución y Frecuencias")
        self.panel_graficos.pack(fill="both", expand=True)

        if not MATPLOTLIB_DISPONIBLE:
            tk.Label(
                self.panel_graficos,
                text="Instale matplotlib (pip install matplotlib) para ver los gráficos.",
                bg=TARJETA, fg=ERROR, font=(FUENTE, 10, "bold")
            ).pack(pady=20)

    def mostrar_cargando(self):
        self.lbl_resumen.configure(
            text="● Calculando estadísticas descriptivas en segundo plano...", fg=AVISO
        )

    @staticmethod
    def calcular_resumen(df):
        """Calcula métricas y datos para gráficos fuera del hilo de Tkinter."""
        total_filas = len(df)
        total_cols = len(df.columns)
        resumen_numerico = []
        col_hist_nombre = None
        col_hist_valores = None

        # Detectar columnas numéricas (convirtiendo desde dtype=object)
        for col in df.columns:
            serie = pd.to_numeric(df[col], errors="coerce")
            validos = int(serie.notna().sum())
            if validos > 0 and (validos / max(total_filas, 1)) >= 0.5:
                resumen_numerico.append({
                    "columna": col,
                    "validos": validos,
                    "promedio": float(serie.mean()),
                    "minimo": float(serie.min()),
                    "maximo": float(serie.max()),
                    "std": float(serie.std()) if validos > 1 else 0.0,
                })
                if col_hist_nombre is None or "caudal" in col.lower():
                    if "id" not in col.lower():
                        col_hist_nombre = col
                        col_hist_valores = serie.dropna().values

        # Detectar columna categórica para gráfico de frecuencias (Top 6)
        col_cat_nombre = None
        cat_conteos = None
        for col in df.columns:
            if any(r["columna"] == col for r in resumen_numerico) or "fecha" in col.lower():
                continue
            unicos = df[col].dropna().nunique()
            if 1 < unicos <= 50:
                col_cat_nombre = col
                cat_conteos = df[col].dropna().astype(str).value_counts().head(6)
                break
        if col_cat_nombre is None:
            for col in df.columns:
                if not any(r["columna"] == col for r in resumen_numerico) and "fecha" not in col.lower():
                    col_cat_nombre = col
                    cat_conteos = df[col].dropna().astype(str).value_counts().head(6)
                    break

        return {
            "total_filas": total_filas,
            "total_cols": total_cols,
            "numericas": resumen_numerico,
            "hist_col": col_hist_nombre,
            "hist_vals": col_hist_valores,
            "cat_col": col_cat_nombre,
            "cat_conteos": cat_conteos,
        }

    def actualizar_vista(self, datos):
        """Dibuja las métricas y gráficos en el hilo principal de Tkinter."""
        self.lbl_resumen.configure(
            text=f"● Total analizado: {miles(datos['total_filas'])} registros · "
                 f"{datos['total_cols']} columnas · {len(datos['numericas'])} variables numéricas.",
            fg=EXITO
        )

        self.tabla_stats.delete(*self.tabla_stats.get_children())
        for item in datos["numericas"]:
            self.tabla_stats.insert("", "end", values=(
                item["columna"],
                miles(item["validos"]),
                f"{item['promedio']:.2f}",
                f"{item['minimo']:.2f}",
                f"{item['maximo']:.2f}",
                f"{item['std']:.2f}",
            ))

        if not MATPLOTLIB_DISPONIBLE:
            return

        if self._canvas is not None:
            self._canvas.get_tk_widget().destroy()
            self._canvas = None

        fig = Figure(figsize=(9, 3.6), dpi=95, facecolor=TARJETA)
        ax1 = fig.add_subplot(121)
        ax2 = fig.add_subplot(122)

        # Gráfico 1: Histograma de variable numérica
        if datos["hist_col"] is not None and len(datos["hist_vals"]) > 0:
            vals = datos["hist_vals"]
            p98 = float(pd.Series(vals).quantile(0.98))
            vals_plot = vals[vals <= p98] if p98 > 0 else vals
            ax1.hist(vals_plot, bins=20, color=ACENTO, edgecolor="white", alpha=0.85)
            ax1.set_title(f"Distribución: {datos['hist_col']} (hasta p98)", fontsize=9, fontweight="bold", color=TEXTO)
            ax1.set_xlabel(datos["hist_col"], fontsize=8)
            ax1.set_ylabel("Frecuencia", fontsize=8)
            ax1.grid(axis="y", linestyle="--", alpha=0.4)
        else:
            ax1.text(0.5, 0.5, "Sin variables numéricas", ha="center", va="center", color=SUAVE)
            ax1.set_axis_off()

        # Gráfico 2: Barras de frecuencias por categoría
        if datos["cat_col"] is not None and datos["cat_conteos"] is not None and len(datos["cat_conteos"]) > 0:
            etiquetas = [e[:18] + "…" if len(e) > 18 else e for e in datos["cat_conteos"].index]
            valores = datos["cat_conteos"].values
            ax2.barh(etiquetas[::-1], valores[::-1], color=ACENTO_OSCURO, alpha=0.85)
            ax2.set_title(f"Top registros por {datos['cat_col']}", fontsize=9, fontweight="bold", color=TEXTO)
            ax2.set_xlabel("Cantidad de registros", fontsize=8)
            ax2.grid(axis="x", linestyle="--", alpha=0.4)
        else:
            ax2.text(0.5, 0.5, "Sin variables categóricas", ha="center", va="center", color=SUAVE)
            ax2.set_axis_off()

        fig.tight_layout(pad=2.0)
        self._canvas = FigureCanvasTkAgg(fig, master=self.panel_graficos)
        self._canvas.draw()
        self._canvas.get_tk_widget().pack(fill="both", expand=True)


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
        self.servidor_sensores = None
        self.detener_escucha = threading.Event()
        self.cola = queue.Queue()
        self._cerrando = False
        self._after_cola = None
        self._hilo_escucha = None
        self._hilo_escucha_sensores = None
        self._hilo_cliente = None
        self._hilos_clientes = set()
        self._hilos_clientes_lock = threading.Lock()
        self._hilo_filtro = None
        self._version_filtro = 0
        self.mongo = MongoRepositorio()
        self._ultima_persistencia = None
        self._sensores_recibidos = 0
        self._sensores_lock = threading.Lock()
        self._escuchas_finalizadas = 0
        self._escuchas_lock = threading.Lock()

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
        estilo.configure("TNotebook", background=FONDO, borderwidth=0, tabmargins=(0, 0, 0, 0))
        estilo.configure("TNotebook.Tab", font=(FUENTE, 10, "bold"), padding=(18, 7),
                         background="#DDE3E9", foreground=SUAVE, borderwidth=0)
        estilo.map("TNotebook.Tab", background=[("selected", TARJETA)],
                   foreground=[("selected", ACENTO_OSCURO)])

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
        self.var_puerto_sensores = tk.StringVar(value=str(PUERTO_SENSORES))
        ttk.Label(cont_red, text="IP", style="Suave.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Entry(cont_red, textvariable=self.var_ip, width=16).grid(row=0, column=1, padx=(6, 14))
        ttk.Label(cont_red, text="Puerto", style="Suave.TLabel").grid(row=0, column=2, sticky="w")
        ttk.Entry(cont_red, textvariable=self.var_puerto, width=7).grid(row=0, column=3, padx=(6, 14))
        ttk.Label(cont_red, text="Sensores", style="Suave.TLabel").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(cont_red, textvariable=self.var_puerto_sensores, width=7).grid(row=1, column=1, padx=(6, 14), pady=(8, 0))
        ttk.Label(cont_red, text="CSV: 12345 · Sensores: 12346", style="Suave.TLabel").grid(
            row=1, column=2, columnspan=2, sticky="w", pady=(8, 0))

        self.boton_red = ttk.Button(cont_red, text="Iniciar escucha", style="Acento.TButton", command=self.alternar_servidor)
        self.boton_red.grid(row=0, column=4, sticky="e")
        cont_red.columnconfigure(4, weight=1)

        self.estado = tk.Label(panel_red, text="● Servidor detenido", bg=TARJETA, fg=SUAVE, font=(FUENTE, 10, "bold"), anchor="w")
        self.estado.pack(anchor="w", pady=(10, 0))

        # Pestañas principales
        self.pestanas = ttk.Notebook(cuerpo)
        self.pestanas.pack(fill="both", expand=True)

        tab_tabla = ttk.Frame(self.pestanas, style="Fondo.TFrame", padding=(0, 8, 0, 0))
        self.dashboard = DashboardEstadistico(self.pestanas)
        self.pestanas.add(tab_tabla, text="Tabla y Filtros (CSV)")
        self.pestanas.add(self.dashboard, text="Dashboard Estadístico")

        # SECCIÓN 2: Filtros de Vista
        panel_filtro = Tarjeta(tab_tabla, "Filtro de búsqueda")
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
        panel_pag = ttk.Frame(tab_tabla, style="Fondo.TFrame")
        panel_pag.pack(fill="x", pady=(0, 5))

        self.btn_anterior = ttk.Button(panel_pag, text="<< Anterior", command=self.pagina_anterior, state="disabled")
        self.btn_anterior.pack(side="left")

        self.lbl_pagina = tk.Label(panel_pag, text="Esperando datos...", bg=FONDO, fg=TEXTO, font=(FUENTE, 10, "bold"))
        self.lbl_pagina.pack(side="left", padx=15)

        self.btn_siguiente = ttk.Button(panel_pag, text="Siguiente >>", command=self.pagina_siguiente, state="disabled")
        self.btn_siguiente.pack(side="left")

        # SECCIÓN 4: Tabla (Treeview)
        panel_tabla = Tarjeta(tab_tabla, "")
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
        host = self.var_ip.get().strip()
        texto_puerto = self.var_puerto.get().strip()
        texto_puerto_sensores = self.var_puerto_sensores.get().strip()
        puertos_validos = all(
            texto.isdigit() and 0 < int(texto) < 65536
            for texto in (texto_puerto, texto_puerto_sensores)
        )
        if not host or not puertos_validos or int(texto_puerto) == int(texto_puerto_sensores):
            self._mostrar_estado("● Revise IP y puertos distintos (1-65535)", ERROR)
            return

        serv_socket = None
        sensores_socket = None
        try:
            serv_socket = socket.create_server((host, int(texto_puerto)))
            sensores_socket = socket.create_server((host, int(texto_puerto_sensores)))
        except OSError as error:
            if serv_socket is not None:
                serv_socket.close()
            if sensores_socket is not None:
                sensores_socket.close()
            motivo = "el puerto ya está en uso" if error.errno in (10048, 98) else str(error)
            self._mostrar_estado(f"● Error al abrir puertos: {motivo}", ERROR)
            return

        serv_socket.settimeout(0.5)
        sensores_socket.settimeout(0.5)
        self.servidor = serv_socket
        self.servidor_sensores = sensores_socket
        self.detener_escucha.clear()

        with self._escuchas_lock:
            self._escuchas_finalizadas = 0
        self._hilo_escucha = threading.Thread(
            target=self._bucle_escucha, args=(serv_socket, "CSV"), daemon=True)
        self._hilo_escucha_sensores = threading.Thread(
            target=self._bucle_escucha, args=(sensores_socket, "SENSORES"), daemon=True)
        self._hilo_escucha.start()
        self._hilo_escucha_sensores.start()

        self.boton_red.configure(text="Detener escucha", style="Alto.TButton")
        self._mostrar_estado(
            f"● CSV en {host}:{texto_puerto} · sensores en {host}:{texto_puerto_sensores}", EXITO)

    def detener(self):
        if self.servidor is not None:
            self.detener_escucha.set()
            self.boton_red.configure(state="disabled")
            self._mostrar_estado("● Deteniendo servidor...", AVISO)

    def _al_detenerse(self):
        self.servidor = None
        self.servidor_sensores = None
        self.boton_red.configure(text="Iniciar escucha", style="Acento.TButton", state="normal")
        self._mostrar_estado("● Servidor detenido", SUAVE)

    def _bucle_escucha(self, servidor, tipo):
        try:
            while not self.detener_escucha.is_set():
                try:
                    conexion, direccion = servidor.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                if self.detener_escucha.is_set():
                    try:
                        respuesta = {"ok": False, "filas": 0,
                                     "detalle": "Servidor deteniendose; reintente."}
                        if tipo == "SENSORES":
                            responder_linea(conexion, {**respuesta, "type": "ERROR"})
                        else:
                            responder(conexion, respuesta)
                    except OSError:
                        pass
                    finally:
                        conexion.close()
                    continue
                self.cola.put(lambda ip=direccion[0]: self._mostrar_estado(
                    f"● Recibiendo {tipo.lower()} de {ip}...", AVISO))
                trabajador = self._atender_cliente if tipo == "CSV" else self._atender_sensores
                hilo = threading.Thread(
                    target=trabajador, args=(conexion,), daemon=True,
                    name=f"{tipo.lower()}-{direccion[0]}-{direccion[1]}")
                with self._hilos_clientes_lock:
                    self._hilos_clientes.add(hilo)
                    self._hilo_cliente = hilo  # Compatibilidad con la API historica.
                hilo.start()
        finally:
            servidor.close()
            with self._hilos_clientes_lock:
                hilos = list(self._hilos_clientes)
            for hilo in hilos:
                hilo.join()
            with self._escuchas_lock:
                self._escuchas_finalizadas += 1
                todas_finalizadas = self._escuchas_finalizadas >= 2
            if todas_finalizadas:
                self.cola.put(self._al_detenerse)

    def _atender_cliente(self, conexion):
        try:
            # El puerto CSV conserva el contrato de Evaluacion 2.
            registros = decodificar(recibir_todo(conexion, self.detener_escucha))
            df_nuevo = dataframe_validado(registros)
            del registros
            # La insercion ocurre fuera del hilo de Tkinter para no congelar la GUI.
            # Sin MONGODB_URI configurada, este paso no modifica la demo local.
            persistencia = self.mongo.guardar_dataframe(df_nuevo)
            self._ultima_persistencia = persistencia
            aceptado = threading.Event()
            cancelado = threading.Event()
            decision = threading.Lock()
            resultado = {}

            def aceptar():
                with decision:
                    anterior = self.df_recibido, self.df_actual, self.pagina_actual
                    try:
                        if cancelado.is_set() or self.detener_escucha.is_set() or self._cerrando:
                            raise ValueError("Transferencia cancelada: servidor detenido o aceptación vencida.")
                        self._datos_recibidos(df_nuevo)
                        if persistencia["habilitado"]:
                            self._mostrar_estado_datos(
                                f"● Datos recibidos: {miles(len(self.df_recibido))} registros. "
                                f"MongoDB: {miles(persistencia['insertados'])} guardados.", EXITO)
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
            hilo_actual = threading.current_thread()
            with self._hilos_clientes_lock:
                self._hilos_clientes.discard(hilo_actual)

    def _atender_sensores(self, conexion):
        """Atiende el puerto dedicado de sensores sin tocar el dashboard CSV."""
        try:
            cantidad = recibir_sensores(conexion, self._atender_sensor, self.detener_escucha)
            responder_linea(conexion, {
                "ok": True,
                "type": "SENSOR_STREAM_ACK",
                "sensores": cantidad,
            })
        except Exception as error:
            try:
                responder_linea(conexion, {
                    "ok": False,
                    "type": "ERROR",
                    "detalle": str(error),
                })
            except OSError:
                pass
            self.cola.put(lambda texto=str(error): self._mostrar_estado(
                f"● Error en sensores: {texto}", ERROR))
        finally:
            conexion.close()
            hilo_actual = threading.current_thread()
            with self._hilos_clientes_lock:
                self._hilos_clientes.discard(hilo_actual)

    def _atender_sensor(self, mensaje, conexion):
        """Valida, persiste y confirma una lectura sin bloquear el CSV."""
        try:
            temperatura = mensaje.get("temperature", mensaje.get("temp"))
            humedad = mensaje.get("humidity", mensaje.get("hum"))
            gas = mensaje.get("gas")
            valores = {
                "temperatura": float(temperatura),
                "humedad": float(humedad),
                "gas": float(gas),
            }
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"Lectura SENSOR invalida: {error}") from error
        if not all(math.isfinite(valor) for valor in valores.values()):
            raise ValueError("La lectura SENSOR contiene un numero no finito.")
        persistencia = self.mongo.guardar_sensor(
            valores["temperatura"], valores["humedad"], valores["gas"],
            mensaje.get("timestamp", mensaje.get("fecha_hora"))
        )
        with self._sensores_lock:
            self._sensores_recibidos += 1
            total = self._sensores_recibidos
        try:
            responder_linea(conexion, {
                "ok": True,
                "type": "SENSOR_ACK",
                "persisted": persistencia["habilitado"],
                "sensores": total,
            })
        except OSError:
            pass
        self.cola.put(lambda total=total: self._mostrar_estado(
            f"● Sensor recibido: {total} lectura(s).", EXITO))

    def _datos_recibidos(self, df_nuevo):
        self._version_filtro += 1
        self._orden = None
        self.entry_filtro.delete(0, tk.END)
        self.df_recibido = df_nuevo
        self.df_actual = self.df_recibido
        self.pagina_actual = 0

        self.cargar_datos_en_tabla()

        self._mostrar_estado_datos(f"● Datos recibidos: {miles(len(self.df_recibido))} registros.", EXITO)
        self.btn_filtrar.configure(state="normal")
        self.btn_limpiar.configure(state="normal")

        # Calcular estadísticas descriptivas del CSV en segundo plano
        self.dashboard.mostrar_cargando()
        threading.Thread(
            target=self._calcular_dashboard, args=(df_nuevo,), daemon=True
        ).start()

    def _calcular_dashboard(self, df):
        try:
            resumen = DashboardEstadistico.calcular_resumen(df)
            self.cola.put(lambda: None if self._cerrando else self.dashboard.actualizar_vista(resumen))
        except Exception as error:
            mensaje = str(error)
            self.cola.put(lambda: None if self._cerrando else self.dashboard.lbl_resumen.configure(
                text=f"● Error al calcular estadísticas: {mensaje}", fg=ERROR
            ))

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

        inicio = self.pagina_actual * self.registros_por_pagina
        fin = inicio + self.registros_por_pagina
        df_pagina = self.df_actual.iloc[inicio:fin]

        filas_valores = df_pagina.astype(object).where(df_pagina.notna(), "—").values.tolist()
        for fila in filas_valores:
            self.tabla.insert("", "end", values=fila)

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
        hilos = (
            self._hilo_escucha,
            self._hilo_escucha_sensores,
            self._hilo_cliente,
            self._hilo_filtro,
        )
        with self._hilos_clientes_lock:
            hilos_clientes = tuple(self._hilos_clientes)
        if any(h is not None and h.is_alive() for h in hilos) or any(
            hilo.is_alive() for hilo in hilos_clientes
        ):
            self.after(50, self._esperar_cierre)
        else:
            while not self.cola.empty():
                self.cola.get_nowait()
            self.mongo.cerrar()
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
        try:
            registros = decodificar(recibir_todo(conexion, self.detener_escucha))
            df = dataframe_validado(registros)
            self.cola.put(lambda: self._datos_recibidos(df))
            responder(conexion, {"ok": True, "filas": len(df), "detalle": "Recepción correcta."})
        except Exception as error:
            try:
                responder(conexion, {"ok": False, "filas": 0, "detalle": str(error)})
            except OSError:
                pass
        finally:
            conexion.close()
            hilo_actual = threading.current_thread()
            with self._hilos_clientes_lock:
                self._hilos_clientes.discard(hilo_actual)

    def _datos_recibidos(self, df_nuevo):
        super()._datos_recibidos(df_nuevo)
        self.registro.configure(state="normal")
        self.registro.insert("end", f"{len(df_nuevo)} filas recibidas\n")
        self.registro.configure(state="disabled")


if __name__ == "__main__":
    ServidorBigData().mainloop()