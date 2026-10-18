"""
CLIENTE (Grupo 3) - Envío de un CSV cualquiera a un servidor por sockets TCP.

Flujo (cuatro pasos, igual que en la pantalla):
    1. Cargar un CSV.               -> recién ahí aparecen sus columnas.
    2. Revisión del archivo.        -> informe de calidad.
    3. Elegir columnas a enviar.    -> una casilla por columna del CSV.
    4. Enviar al servidor.          -> por lotes, con barra de progreso.

Contrato con el servidor (el mismo del demo del Grupo 4):
    - El cliente envía UN arreglo JSON de registros, por ejemplo [{"a":1},{"a":2}]
    - Al final agrega la marca <EOF> y cierra su lado de la conexión.
    - El servidor lee hasta que la conexión se cierra.
    - (Opcional) El servidor puede responder una línea JSON: {"ok": true, "filas": N}

Clases:
    ErrorCliente    -> excepción base; ErrorArchivo y ErrorRed heredan de ella.
    Dataset         -> guarda el CSV cargado, lo revisa y entrega las columnas elegidas.
    EmisorTCP       -> abre el socket y envía los lotes.
    Tarjeta         -> panel visual reutilizable (hereda de ttk.Frame).
    VentanaCliente  -> la GUI (hereda de tk.Tk); solo une las piezas anteriores.

Requisitos: Python 3.10+ con pandas instalado (pip install pandas).
Ejecutar:  python cliente.py
"""

import json
import math
import os
import queue
import socket
import threading
import time
import tkinter as tk
from tkinter import filedialog, ttk

import pandas as pd

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

# Valores iniciales de red. Se pueden cambiar en la pantalla o con variables de entorno.
HOST = os.environ.get("SERVIDOR_HOST", "127.0.0.1")
PUERTO = int(os.environ.get("SERVIDOR_PUERTO", "12345"))

MARCA_FIN = "<EOF>"                                   # marca de fin que espera el servidor
TAMANOS_LOTE = {"Pequeño (1.000)": 1000, "Medio (5.000)": 5000, "Grande (20.000)": 20000}
FILAS_VISTA = 50        # filas de la vista previa
COLUMNAS_POR_FILA = 3   # casillas por fila en la pantalla
TIEMPO_CONEXION = 3     # segundos para conectar
TIEMPO_RESPUESTA = 60   # segundos máximos sin avance (enviando o esperando la respuesta)
PASO_ESPERA = 0.2       # segundos por espera; entre pasos se revisa si el usuario canceló


# ---------------------------------------------------------------------------
# Errores propios (herencia: todos son un ErrorCliente)
# ---------------------------------------------------------------------------

class ErrorCliente(Exception):
    """Base de nuestros errores: la GUI solo captura esta clase."""


class ErrorArchivo(ErrorCliente):
    """El CSV no se puede usar."""


class ErrorRed(ErrorCliente):
    """Falló la conexión o el envío."""


class EnvioCancelado(ErrorCliente):
    """El usuario pulsó 'Cancelar envío'. No es un fallo: es una decisión."""


# Propósito: mostrar 569250 como 569.250 (formato chileno).
def miles(numero):
    return f"{numero:,}".replace(",", ".")


# ---------------------------------------------------------------------------
# Datos: cargar, revisar y elegir columnas (sin GUI ni red)
# ---------------------------------------------------------------------------

class Dataset:
    """
    Guarda el CSV cargado. No depende de las columnas de un archivo en particular,
    así que sirve para cualquier CSV.
    """

    def __init__(self):
        self.datos = None     # DataFrame de pandas
        self.informe = ""     # texto de la revisión

    # Propósito: leer el CSV y preparar el informe. Lanza ErrorArchivo si no sirve.
    def cargar(self, ruta):
        try:
            datos = pd.read_csv(ruta, encoding="utf-8-sig")
        except (OSError, UnicodeError, pd.errors.ParserError, pd.errors.EmptyDataError) as error:
            raise ErrorArchivo(f"No se pudo leer el CSV: {error}") from error
        datos.columns = [str(c).strip() for c in datos.columns]
        if datos.empty:
            raise ErrorArchivo("El CSV no tiene registros.")
        if datos.columns.duplicated().any():
            raise ErrorArchivo("El CSV tiene nombres de columna repetidos.")
        self.datos = datos
        self.informe = self._armar_informe()

    # Propósito: resumen de calidad del archivo. Solo informa: no borra ni cambia datos.
    def _armar_informe(self):
        nulos = {c: int(n) for c, n in self.datos.isna().sum().items() if n}
        return "\n".join([
            f"Filas: {miles(len(self.datos))}   Columnas: {len(self.datos.columns)}",
            "Vacíos: " + (", ".join(f"{c}={miles(n)}" for c, n in nulos.items()) or "ninguno"),
            f"Filas idénticas repetidas: {miles(int(self.datos.duplicated().sum()))}",
            "Los vacíos se envían como null. La revisión informa; no modifica datos.",
        ])

    # Propósito: devolver solo las columnas elegidas (todas las filas).
    def seleccionar(self, columnas):
        if not columnas:
            raise ErrorCliente("Debe quedar al menos una columna seleccionada.")
        return self.datos[columnas]


# ---------------------------------------------------------------------------
# Red
# ---------------------------------------------------------------------------

class EmisorTCP:
    """
    Envía un DataFrame por socket, por lotes, siguiendo el contrato del servidor.
    Se usa un EmisorTCP nuevo por cada envío; otro hilo puede llamar a cancelar().
    """

    def __init__(self):
        self._cancelado = threading.Event()   # se activa cuando el usuario cancela

    # Propósito: pedir que el envío se detenga. Puede llamarse desde otro hilo (el de la ventana).
    # No corta el socket "a la fuerza": el hilo de envío espera en pasos cortos (PASO_ESPERA)
    # y revisa esta marca entre paso y paso. Así la cancelación funciona igual en todos los sistemas.
    def cancelar(self):
        self._cancelado.set()

    # Propósito: ejecutar el envío completo. 'progreso' recibe (lote_actual, total_lotes).
    # Devuelve la respuesta del servidor (dict) o None si el servidor no responde nada.
    # Lanza EnvioCancelado si el usuario cancela, o ErrorRed si algo falla.
    def enviar(self, datos, host, puerto, tam_lote, progreso=lambda actual, total: None):
        if datos.empty:
            raise ErrorRed("No hay filas para enviar.")
        total_lotes = math.ceil(len(datos) / tam_lote)
        try:
            with self._conectar(host, puerto) as conexion:
                conexion.settimeout(PASO_ESPERA)  # cada send/recv espera como máximo un paso corto
                # Armamos UN arreglo JSON grande, pero lo mandamos en trozos:
                # "[" + lote1 + "," + lote2 + ... + "]" + <EOF>. Así nunca tenemos
                # todo el texto en memoria y la barra puede avanzar lote a lote.
                self._enviar_todo(conexion, b"[")
                for numero in range(total_lotes):
                    trozo = datos.iloc[numero * tam_lote:(numero + 1) * tam_lote]
                    registros = self._a_json(trozo)
                    self._enviar_todo(conexion, (("," if numero else "") + registros).encode("utf-8"))
                    progreso(numero + 1, total_lotes)
                self._enviar_todo(conexion, ("]" + MARCA_FIN).encode("utf-8"))
                # Avisamos "no enviaré más": el servidor lee hasta que el cliente cierra.
                conexion.shutdown(socket.SHUT_WR)
                return self._leer_respuesta(conexion)
        except (OSError, ValueError) as error:  # incluye timeouts y puertos mal escritos
            if self._cancelado.is_set():  # cancelar() cortó el socket: el error es esperado
                raise EnvioCancelado("Envío cancelado.") from None
            raise ErrorRed(f"Error de red con {host}:{puerto}: {error}") from error

    # Propósito: enviar todos los bytes, revisando la cancelación cada PASO_ESPERA.
    # Si el servidor no lee, el buffer se llena y send() espera: en vez de quedarnos
    # bloqueados, reintentamos en pasos cortos. Si pasa TIEMPO_RESPUESTA sin avanzar, nos rendimos.
    def _enviar_todo(self, conexion, datos):
        vista, enviados = memoryview(datos), 0
        ultimo_avance = time.monotonic()
        while enviados < len(vista):
            if self._cancelado.is_set():
                raise EnvioCancelado("Envío cancelado.")
            try:
                enviados += conexion.send(vista[enviados:])
                ultimo_avance = time.monotonic()
            except TimeoutError:
                if time.monotonic() - ultimo_avance > TIEMPO_RESPUESTA:
                    raise TimeoutError("El servidor dejó de recibir datos.") from None

    # Propósito: conectar con el servidor sin quedar "pegado" si el usuario cancela.
    # En vez de una sola espera de 3 s, se intenta en pasos de 0,3 s revisando la cancelación.
    # Bonus: si el servidor se inicia durante esos 3 s, se conecta igual.
    def _conectar(self, host, puerto):
        limite = time.monotonic() + TIEMPO_CONEXION
        while not self._cancelado.is_set() and time.monotonic() < limite:
            try:
                return socket.create_connection(
                    (host, puerto), timeout=min(0.3, max(0.05, limite - time.monotonic())))
            except TimeoutError:
                continue             # aún sin respuesta: reintentar
            except ConnectionRefusedError:
                break                # nadie escucha en ese puerto
        if self._cancelado.is_set():
            raise EnvioCancelado("Envío cancelado.")
        raise ErrorRed(f"No hay un servidor escuchando en {host}:{puerto}. Inícielo primero.")

    # Propósito: convertir un trozo del DataFrame en texto JSON de registros, SIN los corchetes.
    # - json.dumps escapa las tildes (como ó): el texto viaja en ASCII puro y ningún
    #   carácter se parte entre dos recv() del servidor.
    # - Los números salen con su representación más corta que recupera el valor exacto
    #   (to_json de pandas, en cambio, redondea a 10 decimales).
    # - Los vacíos (NaN) se cambian por None, que JSON escribe como null.
    @staticmethod
    def _a_json(trozo):
        filas = trozo.astype(object).where(trozo.notna(), None).to_dict("records")
        return json.dumps(filas, allow_nan=False, separators=(",", ":"))[1:-1]

    # Propósito: leer la confirmación opcional del servidor (una línea JSON) hasta que cierre.
    # También espera en pasos cortos para poder cancelar; tras TIEMPO_RESPUESTA se rinde.
    def _leer_respuesta(self, conexion):
        partes, recibido = [], 0
        limite = time.monotonic() + TIEMPO_RESPUESTA
        while recibido < 65536:
            if self._cancelado.is_set():
                raise EnvioCancelado("Envío cancelado.")
            if time.monotonic() > limite:
                raise TimeoutError("El servidor no respondió a tiempo.")
            try:
                dato = conexion.recv(4096)
            except TimeoutError:
                continue  # aún no llega nada: revisar la cancelación y seguir esperando
            except ConnectionResetError:
                return None  # el servidor cerró de golpe: los datos ya se enviaron
            if not dato:
                break
            partes.append(dato)
            recibido += len(dato)
        try:
            return json.loads(b"".join(partes)) if partes else None
        except ValueError:
            return None


# ---------------------------------------------------------------------------
# Interfaz gráfica (Tkinter)
# ---------------------------------------------------------------------------

# Paleta sobria con acento "agua".
FONDO, TARJETA, TEXTO, SUAVE = "#EEF2F5", "#FFFFFF", "#1F2A37", "#6B7785"
ACENTO, ACENTO_OSCURO, EXITO, ERROR = "#0E7490", "#155E75", "#15803D", "#B91C1C"
FUENTE = "Segoe UI"


class Tarjeta(ttk.Frame):
    """Panel blanco con título. Evita repetir código para cada sección."""

    def __init__(self, padre, titulo):
        super().__init__(padre, style="Tarjeta.TFrame", padding=14)
        ttk.Label(self, text=titulo, style="Titulo.TLabel").pack(anchor="w", pady=(0, 8))


class VentanaCliente(tk.Tk):
    """
    La ventana. No sabe leer CSV ni usar sockets: delega en Dataset y EmisorTCP.
    Solo muestra información y recoge lo que el usuario elige.
    """

    def __init__(self):
        super().__init__()
        self.title("Cliente · Envío de datos por sockets")
        self.geometry("1120x740")
        self.minsize(980, 640)
        self.configure(bg=FONDO)

        self.dataset = Dataset()
        self.var_columnas = {}            # columna -> BooleanVar (¿se envía?)
        self.emisor = None                # el EmisorTCP del envío en curso (None = no hay envío)
        self.cola = queue.Queue()         # los hilos dejan aquí tareas para la GUI
        self.cargado = False

        self._crear_estilos()
        self._crear_widgets()
        self.after(100, self._revisar_cola)

    # Propósito: definir colores y tipografía de los widgets ttk.
    def _crear_estilos(self):
        estilo = ttk.Style(self)
        estilo.theme_use("clam")  # 'clam' permite personalizar colores
        estilo.configure(".", font=(FUENTE, 10), background=TARJETA, foreground=TEXTO)
        estilo.configure("Tarjeta.TFrame", background=TARJETA)
        estilo.configure("Fondo.TFrame", background=FONDO)
        estilo.configure("Titulo.TLabel", font=(FUENTE, 12, "bold"), background=TARJETA)
        estilo.configure("Suave.TLabel", foreground=SUAVE, background=TARJETA)
        estilo.configure("TCheckbutton", background=TARJETA)
        estilo.configure("TButton", padding=(12, 6), background="#E3E8ED", borderwidth=0)
        estilo.map("TButton", background=[("active", "#D3DBE2"), ("disabled", "#F1F4F6")])
        estilo.configure("Acento.TButton", background=ACENTO, foreground="white",
                         font=(FUENTE, 10, "bold"))
        estilo.map("Acento.TButton", background=[("active", ACENTO_OSCURO), ("disabled", "#A5C8D2")],
                   foreground=[("disabled", "#F4F8F9")])
        estilo.configure("Peligro.TButton", background=ERROR, foreground="white",
                         font=(FUENTE, 10, "bold"))
        estilo.map("Peligro.TButton", background=[("active", "#991B1B"), ("disabled", "#E5B4B4")],
                   foreground=[("disabled", "#FBF1F1")])
        estilo.configure("Horizontal.TProgressbar", troughcolor="#E3E8ED", background=ACENTO,
                         borderwidth=0, thickness=8)
        estilo.configure("Treeview", rowheight=22, fieldbackground=TARJETA, borderwidth=0)
        estilo.configure("Treeview.Heading", font=(FUENTE, 9, "bold"), background="#E3E8ED")

    # Propósito: construir la pantalla. Izquierda: pasos 1, 2 y 4. Derecha: paso 3 y vista previa.
    def _crear_widgets(self):
        encabezado = ttk.Frame(self, style="Fondo.TFrame", padding=(20, 16, 20, 4))
        encabezado.pack(fill="x")
        tk.Label(encabezado, text="Cliente de envío de datos", bg=FONDO, fg=TEXTO,
                 font=(FUENTE, 20, "bold")).pack(anchor="w")
        tk.Label(encabezado, text="Cargue un CSV, elija las columnas y transmítalo por sockets TCP",
                 bg=FONDO, fg=SUAVE, font=(FUENTE, 10)).pack(anchor="w")

        cuerpo = ttk.Frame(self, style="Fondo.TFrame", padding=(20, 8, 20, 16))
        cuerpo.pack(fill="both", expand=True)
        cuerpo.columnconfigure(0, weight=2, uniform="col")
        cuerpo.columnconfigure(1, weight=3, uniform="col")
        cuerpo.rowconfigure(0, weight=1)
        izquierda = ttk.Frame(cuerpo, style="Fondo.TFrame")
        derecha = ttk.Frame(cuerpo, style="Fondo.TFrame")
        izquierda.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        derecha.grid(row=0, column=1, sticky="nsew", padx=(8, 0))

        self._crear_paso_cargar(izquierda)
        self._crear_paso_revision(izquierda)
        self._crear_paso_enviar(izquierda)
        self._crear_paso_columnas(derecha)
        self._crear_vista_previa(derecha)

    # Propósito: paso 1, botón para elegir el CSV.
    def _crear_paso_cargar(self, padre):
        tarjeta = Tarjeta(padre, "1 · Cargar archivo")
        tarjeta.pack(fill="x", pady=(0, 12))
        self.boton_cargar = ttk.Button(tarjeta, text="Elegir CSV…", style="Acento.TButton",
                                       command=self.elegir_csv)
        self.boton_cargar.pack(anchor="w")
        self.etiqueta_ruta = ttk.Label(tarjeta, text="Ningún archivo seleccionado",
                                       style="Suave.TLabel", wraplength=360)
        self.etiqueta_ruta.pack(anchor="w", pady=(8, 0))
        self.barra_carga = ttk.Progressbar(tarjeta, mode="indeterminate")

    # Propósito: paso 2, caja de solo lectura con el informe.
    def _crear_paso_revision(self, padre):
        tarjeta = Tarjeta(padre, "2 · Revisión del archivo")
        tarjeta.pack(fill="both", expand=True, pady=(0, 12))
        self.texto_informe = tk.Text(tarjeta, height=7, wrap="word", relief="flat", bg="#F7F9FA",
                                     fg=TEXTO, font=(FUENTE, 10), padx=10, pady=8, state="disabled")
        self.texto_informe.pack(fill="both", expand=True)
        self._escribir_informe("Cargue un CSV para ver su informe de calidad.")

    # Propósito: paso 4, datos de red, tamaño de lote, botón de envío y progreso.
    def _crear_paso_enviar(self, padre):
        tarjeta = Tarjeta(padre, "4 · Enviar al servidor")
        tarjeta.pack(fill="x")
        red = ttk.Frame(tarjeta, style="Tarjeta.TFrame")
        red.pack(fill="x", pady=(0, 8))
        self.var_ip, self.var_puerto = tk.StringVar(value=HOST), tk.StringVar(value=str(PUERTO))
        ttk.Label(red, text="IP", style="Suave.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Entry(red, textvariable=self.var_ip, width=16).grid(row=0, column=1, padx=(6, 12))
        ttk.Label(red, text="Puerto", style="Suave.TLabel").grid(row=0, column=2, sticky="w")
        ttk.Entry(red, textvariable=self.var_puerto, width=7).grid(row=0, column=3, padx=(6, 0))
        ttk.Label(red, text="Lote", style="Suave.TLabel").grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.combo_lote = ttk.Combobox(red, state="readonly", width=16, values=list(TAMANOS_LOTE))
        self.combo_lote.set("Medio (5.000)")
        self.combo_lote.grid(row=1, column=1, columnspan=2, sticky="w", padx=(6, 0), pady=(6, 0))

        botones = ttk.Frame(tarjeta, style="Tarjeta.TFrame")
        botones.pack(anchor="w")
        self.boton_enviar = ttk.Button(botones, text="Enviar datos", style="Acento.TButton",
                                       command=self.enviar, state="disabled")
        self.boton_enviar.pack(side="left")
        # Solo se habilita mientras hay un envío en curso.
        self.boton_cancelar = ttk.Button(botones, text="Cancelar envío", style="Peligro.TButton",
                                         command=self.cancelar_envio, state="disabled")
        self.boton_cancelar.pack(side="left", padx=8)
        self.barra_envio = ttk.Progressbar(tarjeta, mode="determinate")
        self.barra_envio.pack(fill="x", pady=(10, 4))
        self.etiqueta_envio = ttk.Label(tarjeta, text="Esperando datos…", style="Suave.TLabel",
                                        wraplength=360)
        self.etiqueta_envio.pack(anchor="w")

    # Propósito: paso 3. Una casilla por columna; solo aparecen después de cargar el CSV.
    def _crear_paso_columnas(self, padre):
        self.tarjeta_columnas = Tarjeta(padre, "3 · Columnas a enviar")
        self.tarjeta_columnas.pack(fill="x", pady=(0, 12))
        self.aviso_columnas = ttk.Label(self.tarjeta_columnas, style="Suave.TLabel",
                                        text="Aquí aparecerán las columnas del CSV.")
        self.aviso_columnas.pack(anchor="w")

        self.zona_columnas = ttk.Frame(self.tarjeta_columnas, style="Tarjeta.TFrame")  # aún no visible
        barra = ttk.Frame(self.zona_columnas, style="Tarjeta.TFrame")
        barra.pack(side="bottom", fill="x", pady=(8, 0))
        self.boton_todas = ttk.Button(barra, text="Marcar todas", command=lambda: self.marcar_todas(True))
        self.boton_todas.pack(side="left")
        self.boton_ninguna = ttk.Button(barra, text="Desmarcar todas", command=lambda: self.marcar_todas(False))
        self.boton_ninguna.pack(side="left", padx=6)
        self.etiqueta_seleccion = ttk.Label(barra, text="", style="Suave.TLabel")
        self.etiqueta_seleccion.pack(side="right")

        # Lienzo con scroll: si un CSV tiene muchas columnas, la lista se desplaza.
        self.lienzo = tk.Canvas(self.zona_columnas, bg=TARJETA, highlightthickness=0, height=110)
        scroll = ttk.Scrollbar(self.zona_columnas, orient="vertical", command=self.lienzo.yview)
        self.lienzo.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        self.lienzo.pack(side="left", fill="both", expand=True)
        self.casillas = ttk.Frame(self.lienzo, style="Tarjeta.TFrame")
        self.lienzo.create_window((0, 0), window=self.casillas, anchor="nw")
        self.casillas.bind("<Configure>", lambda e: self.lienzo.configure(
            scrollregion=self.lienzo.bbox("all")))
        self.bind_all("<MouseWheel>", self._rueda)

    # Propósito: mover la lista con la rueda del mouse si el cursor está sobre ella.
    def _rueda(self, evento):
        destino = self.winfo_containing(evento.x_root, evento.y_root)
        if destino is not None and str(destino).startswith(str(self.lienzo)):
            self.lienzo.yview_scroll(-1 if evento.delta > 0 else 1, "units")

    # Propósito: tabla con las primeras filas de las columnas elegidas.
    def _crear_vista_previa(self, padre):
        tarjeta = Tarjeta(padre, f"Vista previa ({FILAS_VISTA} filas)")
        tarjeta.pack(fill="both", expand=True)
        contenedor = ttk.Frame(tarjeta, style="Tarjeta.TFrame")
        contenedor.pack(fill="both", expand=True)
        self.tabla = ttk.Treeview(contenedor, show="headings")
        scroll_y = ttk.Scrollbar(contenedor, orient="vertical", command=self.tabla.yview)
        scroll_x = ttk.Scrollbar(contenedor, orient="horizontal", command=self.tabla.xview)
        self.tabla.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        scroll_y.pack(side="right", fill="y")
        scroll_x.pack(side="bottom", fill="x")
        self.tabla.pack(side="left", fill="both", expand=True)

    # Propósito: reemplazar el texto del informe (la caja está bloqueada, se desbloquea un instante).
    def _escribir_informe(self, texto):
        self.texto_informe.configure(state="normal")
        self.texto_informe.delete("1.0", "end")
        self.texto_informe.insert("1.0", texto)
        self.texto_informe.configure(state="disabled")

    # Propósito: bloquear los botones mientras un hilo trabaja y habilitarlos después.
    def _bloquear(self, bloqueado):
        self.boton_cargar.configure(state="disabled" if bloqueado else "normal")
        listo = "disabled" if bloqueado or not self.cargado else "normal"
        for boton in (self.boton_enviar, self.boton_todas, self.boton_ninguna):
            boton.configure(state=listo)

    # --- Paso 1: cargar -----------------------------------------------------

    # Propósito: elegir el archivo y cargarlo en un hilo aparte (así la ventana no se congela).
    def elegir_csv(self):
        ruta = filedialog.askopenfilename(title="Elegir CSV", filetypes=[("CSV", "*.csv"), ("Todos", "*.*")])
        if not ruta:
            return
        self.etiqueta_ruta.configure(text=ruta)
        self._escribir_informe("Leyendo y revisando el archivo…")
        self.barra_carga.pack(fill="x", pady=(8, 0))
        self.barra_carga.start(12)
        self._bloquear(True)
        threading.Thread(target=self._tarea_cargar, args=(ruta,), daemon=True).start()

    # Propósito: (hilo) cargar. No toca widgets: deja una tarea en la cola para la ventana.
    def _tarea_cargar(self, ruta):
        nuevo = Dataset()
        try:
            nuevo.cargar(ruta)
            self.cola.put(lambda: self._al_cargar(nuevo))
        except ErrorCliente as error:
            self.cola.put(lambda: self._al_fallar_carga(str(error)))

    def _al_cargar(self, nuevo):
        self.dataset, self.cargado = nuevo, True
        self.barra_carga.stop()
        self.barra_carga.pack_forget()
        self._escribir_informe(nuevo.informe)
        self._crear_casillas()
        self._bloquear(False)
        self.mostrar_seleccion()
        self.barra_envio.configure(value=0)
        self.etiqueta_envio.configure(text="Listo para enviar.", foreground=SUAVE)

    def _al_fallar_carga(self, texto):
        self.barra_carga.stop()
        self.barra_carga.pack_forget()
        self._escribir_informe(f"⚠ {texto}")
        self._bloquear(False)

    # --- Paso 3: columnas (se crean según el CSV cargado) ---------------------

    # Propósito: una casilla por columna del CSV, todas marcadas al inicio.
    def _crear_casillas(self):
        self.aviso_columnas.pack_forget()
        self.zona_columnas.pack(fill="both", expand=True)
        for widget in self.casillas.winfo_children():  # borrar las del CSV anterior
            widget.destroy()
        self.var_columnas = {}
        for i, columna in enumerate(self.dataset.datos.columns):
            self.var_columnas[columna] = tk.BooleanVar(value=True)
            ttk.Checkbutton(self.casillas, text=columna, variable=self.var_columnas[columna],
                            command=self.mostrar_seleccion).grid(
                row=i // COLUMNAS_POR_FILA, column=i % COLUMNAS_POR_FILA,
                sticky="w", padx=(0, 18), pady=3)

    # Propósito: marcar o desmarcar todas las casillas de una vez.
    def marcar_todas(self, valor):
        for variable in self.var_columnas.values():
            variable.set(valor)
        self.mostrar_seleccion()

    # Propósito: leer las casillas y refrescar la vista previa.
    # Devuelve el DataFrame con las columnas elegidas, o None si no hay ninguna (ya se avisó).
    def mostrar_seleccion(self):
        columnas = [c for c, v in self.var_columnas.items() if v.get()]
        try:
            resultado = self.dataset.seleccionar(columnas)
        except ErrorCliente as error:
            self.etiqueta_seleccion.configure(text=f"⚠ {error}", foreground=ERROR)
            self.tabla.delete(*self.tabla.get_children())
            return None
        self.etiqueta_seleccion.configure(
            text=f"{len(columnas)} de {len(self.var_columnas)} columnas · {miles(len(resultado))} filas",
            foreground=SUAVE)
        self._mostrar_vista_previa(resultado)
        return resultado

    # Propósito: reconstruir la tabla (las columnas cambian según el CSV y la selección).
    def _mostrar_vista_previa(self, resultado):
        muestra = resultado.head(FILAS_VISTA)
        columnas = list(resultado.columns)
        self.tabla.delete(*self.tabla.get_children())
        self.tabla.configure(columns=columnas)
        for columna in columnas:
            self.tabla.heading(columna, text=columna)
            self.tabla.column(columna, width=110, minwidth=70, stretch=True)
        for fila in muestra.astype(object).where(muestra.notna(), "—").values.tolist():
            self.tabla.insert("", "end", values=fila)

    # --- Paso 4: enviar -------------------------------------------------------

    # Propósito: validar la selección y enviar en un hilo aparte.
    def enviar(self):
        resultado = self.mostrar_seleccion()  # reutiliza la validación y refresca la vista previa
        if resultado is None:
            self.etiqueta_envio.configure(text="⚠ Elija al menos una columna.", foreground=ERROR)
            return
        host, texto_puerto = self.var_ip.get().strip(), self.var_puerto.get().strip()
        if not host or not texto_puerto.isdigit() or not 0 < int(texto_puerto) < 65536:
            self.etiqueta_envio.configure(text="⚠ Revise la IP y el puerto (1-65535).", foreground=ERROR)
            return
        self.barra_envio.configure(value=0)
        self.etiqueta_envio.configure(text="Conectando con el servidor…", foreground=SUAVE)
        self._bloquear(True)
        self.boton_cancelar.configure(state="normal")
        tam_lote = TAMANOS_LOTE[self.combo_lote.get()]
        self.emisor = EmisorTCP()
        threading.Thread(target=self._tarea_enviar,
                         args=(self.emisor, resultado, host, int(texto_puerto), tam_lote),
                         daemon=True).start()

    # Propósito: el usuario pulsó "Cancelar envío". Interrumpe la conexión en curso.
    def cancelar_envio(self):
        if self.emisor is not None:
            self.emisor.cancelar()
            self.boton_cancelar.configure(state="disabled")
            self.etiqueta_envio.configure(text="Cancelando…", foreground=SUAVE)

    # Propósito: (hilo) enviar y dejar el avance y el resultado en la cola.
    def _tarea_enviar(self, emisor, datos, host, puerto, tam_lote):
        try:
            respuesta = emisor.enviar(
                datos, host, puerto, tam_lote,
                lambda actual, total: self.cola.put(lambda: self._al_progresar(actual, total)))
            self.cola.put(lambda: self._al_enviar(len(datos), respuesta))
        except EnvioCancelado:
            self.cola.put(self._al_cancelar)
        except ErrorCliente as error:
            # Python borra "error" al salir del except; se guarda el texto antes de que la lambda corra.
            mensaje = str(error)
            self.cola.put(lambda: self._al_fallar_envio(mensaje))

    # Propósito: dejar la pantalla lista para intentar de nuevo (se llama al terminar un envío).
    def _terminar_envio(self):
        self.emisor = None
        self.boton_cancelar.configure(state="disabled")
        self._bloquear(False)

    def _al_progresar(self, actual, total):
        self.barra_envio.configure(maximum=total, value=actual)
        self.etiqueta_envio.configure(text=f"Enviando lote {actual} de {total}…", foreground=SUAVE)

    def _al_enviar(self, filas, respuesta):
        self._terminar_envio()
        if respuesta is None:
            texto = f"✔ {miles(filas)} filas enviadas. El servidor no devolvió confirmación."
            color = EXITO
        elif respuesta.get("ok") and respuesta.get("filas") == filas:
            texto = f"✔ El servidor confirmó la recepción de {miles(filas)} filas."
            color = EXITO
        else:
            texto = f"✖ Respuesta inesperada del servidor: {respuesta}"
            color = ERROR
        self.etiqueta_envio.configure(text=texto, foreground=color)

    def _al_fallar_envio(self, texto):
        self._terminar_envio()
        self.etiqueta_envio.configure(text=f"✖ {texto}", foreground=ERROR)

    def _al_cancelar(self):
        self._terminar_envio()
        self.barra_envio.configure(value=0)
        self.etiqueta_envio.configure(text="Envío cancelado. Puede volver a intentarlo.", foreground=SUAVE)

    # --- Comunicación entre hilos ---------------------------------------------

    # Propósito: cada 100 ms, ejecutar las tareas que dejaron los hilos.
    # Tkinter no es seguro entre hilos: solo el hilo principal puede tocar widgets.
    def _revisar_cola(self):
        try:
            while True:
                self.cola.get_nowait()()
        except queue.Empty:
            pass
        self.after(100, self._revisar_cola)


if __name__ == "__main__":
    VentanaCliente().mainloop()
