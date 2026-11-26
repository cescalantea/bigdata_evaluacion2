"""
SIMULADOR DE ARDUINO - SOLO PARA PRUEBAS (no es un sensor real).

Reemplaza al Arduino cuando no se tiene la placa: abre un socket local y envía, una vez por
segundo, líneas "temperatura,humedad,gas" idénticas a las del script v3 del Arduino.
La aplicación (cliente.py) se conecta eligiendo "socket://127.0.0.1:5577" en la lista de puertos.

Los datos son SINTÉTICOS: modelan un DHT11 y un MQ-2, pero no son mediciones. Por eso toda
exportación lleva el campo origen="simulado" y la semilla, para que quien los analice sepa
de dónde vienen y pueda reproducirlos.

Ejecutar:  python simulador_arduino.py
"""

import csv
import json
import math
import random
import socket
import threading
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox, ttk

from sensor_arduino import PUERTO_SIMULADOR

PERIODO_MS = 1000              # 1 línea por segundo, como el script v3 (límite del DHT11)
PRECALENTAMIENTO_S = 20        # el MQ-2 tarda ~20 s en estabilizarse al encender
ORIGEN = "simulado"
CAMPOS_EXPORTACION = ("fecha_hora", "temperatura", "humedad", "gas", "estado", "origen")
ESTADO_OK, ESTADO_ERROR = "OK", "ERROR_LECTURA"
MAX_LINEAS_MONITOR = 200


# ---------------------------------------------------------------------------
# Generador de datos (sin GUI ni red)
# ---------------------------------------------------------------------------

class GeneradorSensor:
    """
    Produce lecturas con el comportamiento de un DHT11 y un MQ-2.
    Con la misma semilla produce siempre la misma secuencia (reproducible).

    DHT11: temperatura 0-50 °C y humedad 20-90 %, con resolución de 1 unidad (el sensor azul
    entrega enteros, el sketch los imprime como "23.0") y fallos ocasionales (ERROR_LECTURA).
    MQ-2: lectura analógica 0-1023; sube al encender (precalentamiento) y ante un evento de gas.
    """

    def __init__(self, semilla=None, dht_entero=True, prob_error=0.01):
        # Sin semilla se elige una al azar, pero se guarda para poder repetir la corrida.
        self.semilla = semilla if semilla is not None else random.randrange(2 ** 32)
        self._rng = random.Random(self.semilla)
        self.dht_entero = dht_entero
        self.prob_error = prob_error
        self.t = 0               # segundos desde el encendido (un tic = un segundo simulado)
        self._deriva = 0.0       # variación lenta de la temperatura
        self._evento = 0.0       # exceso de gas por un evento, decae con el tiempo

    # Propósito: provocar un aumento brusco de gas (humo, fuga). Decae solo en ~1 minuto.
    def evento_gas(self, intensidad=400):
        self._evento += intensidad

    # Propósito: siguiente lectura {"temperatura", "humedad", "gas"} o None si el DHT11 "falla".
    def siguiente(self):
        r, t = self._rng, self.t
        self._deriva = self._deriva * 0.995 + r.gauss(0, 0.03)
        temp = 22 + 3 * math.sin(2 * math.pi * t / 600) + self._deriva + r.gauss(0, 0.12)
        hum = 55 - 1.2 * (temp - 22) + 4 * math.sin(2 * math.pi * t / 420 + 1) + r.gauss(0, 0.4)
        precalentamiento = 300 * math.exp(-t / 7) if t < PRECALENTAMIENTO_S * 3 else 0.0
        gas = (140 + 15 * math.sin(2 * math.pi * t / 900) + precalentamiento + self._evento
               + r.gauss(0, 2.5))
        falla = r.random() < self.prob_error
        self._evento *= math.exp(-1 / 15)
        self.t += 1
        if falla:
            return None
        temp, hum = min(max(temp, 0.0), 50.0), min(max(hum, 20.0), 90.0)
        if self.dht_entero:
            temp, hum = float(round(temp)), float(round(hum))
        else:
            temp, hum = round(temp, 1), round(hum, 1)
        return {"temperatura": temp, "humedad": hum, "gas": int(min(max(gas, 0), 1023))}

    # Propósito: la línea tal como la imprime el Arduino: "23.0,55.0,134" o "ERROR_LECTURA".
    @staticmethod
    def linea(lectura):
        if lectura is None:
            return ESTADO_ERROR
        return f"{lectura['temperatura']:.1f},{lectura['humedad']:.1f},{lectura['gas']}"


# ---------------------------------------------------------------------------
# Salida "serie" por socket
# ---------------------------------------------------------------------------

class EmisorSerie:
    """
    Servidor TCP local que hace de puerto serie: la aplicación se conecta y recibe las líneas.
    Escucha solo en 127.0.0.1: el simulador no se expone a la red.
    """

    def __init__(self, puerto=PUERTO_SIMULADOR):
        self._puerto_pedido = puerto
        self._escucha = None
        self._cliente = None
        self._candado = threading.Lock()
        self._parar = threading.Event()

    # Propósito: abrir el puerto y empezar a aceptar a la aplicación. Lanza OSError si está ocupado.
    def iniciar(self):
        self._escucha = socket.create_server(("127.0.0.1", self._puerto_pedido))
        self._escucha.settimeout(0.3)
        threading.Thread(target=self._aceptar, daemon=True).start()

    @property
    def puerto(self):
        return self._escucha.getsockname()[1]

    @property
    def hay_cliente(self):
        with self._candado:
            return self._cliente is not None

    # Propósito: (hilo) aceptar conexiones; si llega una nueva, reemplaza a la anterior.
    def _aceptar(self):
        while not self._parar.is_set():
            try:
                conexion, _ = self._escucha.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            conexion.settimeout(1)
            with self._candado:
                self._cerrar_cliente()
                self._cliente = conexion

    # Propósito: enviar una línea terminada en \r\n (como println). False si nadie escucha.
    def enviar(self, linea):
        with self._candado:
            if self._cliente is None:
                return False
            try:
                self._cliente.sendall((linea + "\r\n").encode("ascii"))
                return True
            except OSError:               # la aplicación se desconectó
                self._cerrar_cliente()
                return False

    def _cerrar_cliente(self):
        if self._cliente is not None:
            try:
                self._cliente.close()
            except OSError:
                pass
            self._cliente = None

    # Propósito: cerrar el puerto y la conexión con la aplicación.
    def detener(self):
        self._parar.set()
        with self._candado:
            self._cerrar_cliente()
        if self._escucha is not None:
            self._escucha.close()


# ---------------------------------------------------------------------------
# Exportación (CSV y JSON)
# ---------------------------------------------------------------------------

# Propósito: una fila exportable. Misma marca de tiempo que usa la aplicación (milisegundos).
def armar_registro(momento, lectura):
    base = lectura or {"temperatura": None, "humedad": None, "gas": None}
    return {"fecha_hora": momento.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], **base,
            "estado": ESTADO_OK if lectura else ESTADO_ERROR, "origen": ORIGEN}


# Propósito: CSV con una fila por línea enviada. Los fallos del DHT11 quedan con los valores vacíos.
def exportar_csv(ruta, registros):
    with open(ruta, "w", encoding="utf-8", newline="") as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=CAMPOS_EXPORTACION)
        escritor.writeheader()
        escritor.writerows(registros)


# Propósito: JSON con los metadatos (origen, semilla, parámetros) y los registros.
# Los fallos del DHT11 llevan null en los valores.
def exportar_json(ruta, registros, meta):
    errores = sum(1 for r in registros if r["estado"] != ESTADO_OK)
    documento = {
        "origen": ORIGEN,
        "generador": "simulador_arduino.py",
        "aviso": "Datos sintéticos que modelan un DHT11 y un MQ-2; no son mediciones reales.",
        **meta,
        "resumen": {"registros": len(registros), "errores_lectura": errores,
                    "inicio": registros[0]["fecha_hora"] if registros else None,
                    "fin": registros[-1]["fecha_hora"] if registros else None},
        "registros": registros,
    }
    with open(ruta, "w", encoding="utf-8") as archivo:
        json.dump(documento, archivo, ensure_ascii=False, indent=2, allow_nan=False)


# ---------------------------------------------------------------------------
# Ventana
# ---------------------------------------------------------------------------

FONDO, TARJETA, TEXTO, SUAVE = "#EEF2F5", "#FFFFFF", "#1F2A37", "#6B7785"
ACENTO, EXITO, ERROR, AVISO = "#0E7490", "#15803D", "#B91C1C", "#B45309"
FUENTE = "Segoe UI"


class VentanaSimulador(tk.Tk):
    """Ventana aparte: enciende/apaga el 'Arduino', muestra lo que envía y exporta lo enviado."""

    def __init__(self):
        super().__init__()
        self.title("Simulador de Arduino · solo pruebas")
        self.geometry("640x720")
        self.minsize(580, 660)
        self.configure(bg=FONDO)
        self.generador = None
        self.emisor = None
        self.registros = []          # solo lo que llegó a enviarse a la aplicación
        self.meta = {}
        self.exportado = True
        self._id_tic = None
        self.var_puerto = tk.StringVar(value=str(PUERTO_SIMULADOR))
        self.var_semilla = tk.StringVar()
        self.var_entero = tk.BooleanVar(value=True)
        self._crear_estilos()
        self._crear_widgets()
        self.protocol("WM_DELETE_WINDOW", self._cerrar)

    # Propósito: tema 'clam' con los mismos colores del cliente.
    def _crear_estilos(self):
        estilo = ttk.Style(self)
        estilo.theme_use("clam")
        estilo.configure(".", font=(FUENTE, 10), background=TARJETA, foreground=TEXTO)
        estilo.configure("TButton", padding=(12, 6), background="#E3E8ED", borderwidth=0)
        estilo.map("TButton", background=[("active", "#D3DBE2"), ("disabled", "#F1F4F6")])
        estilo.configure("Acento.TButton", background=ACENTO, foreground="white", font=(FUENTE, 10, "bold"))
        estilo.map("Acento.TButton", background=[("active", "#155E75"), ("disabled", "#A5C8D2")])
        estilo.configure("Peligro.TButton", background=ERROR, foreground="white", font=(FUENTE, 10, "bold"))
        estilo.map("Peligro.TButton", background=[("active", "#991B1B")])
        estilo.configure("TCheckbutton", background=TARJETA)

    # Propósito: una tarjeta blanca con título.
    def _tarjeta(self, titulo):
        marco = tk.Frame(self, bg=TARJETA, padx=14, pady=12)
        marco.pack(fill="x", padx=20, pady=(0, 10))
        tk.Label(marco, text=titulo, bg=TARJETA, fg=TEXTO, font=(FUENTE, 12, "bold")).pack(anchor="w", pady=(0, 8))
        return marco

    def _crear_widgets(self):
        cabecera = tk.Frame(self, bg=FONDO)
        cabecera.pack(fill="x", padx=20, pady=(16, 8))
        tk.Label(cabecera, text="Simulador de Arduino", bg=FONDO, fg=TEXTO,
                 font=(FUENTE, 20, "bold")).pack(anchor="w")
        tk.Label(cabecera, text="Solo para pruebas · los datos son sintéticos (modelo de DHT11 + MQ-2)",
                 bg=FONDO, fg=AVISO, font=(FUENTE, 10)).pack(anchor="w")

        # --- Encendido
        marco = self._tarjeta("Encendido")
        fila = tk.Frame(marco, bg=TARJETA)
        fila.pack(fill="x")
        tk.Label(fila, text="Puerto local", bg=TARJETA, fg=SUAVE).grid(row=0, column=0, sticky="w")
        self.entrada_puerto = ttk.Entry(fila, textvariable=self.var_puerto, width=7)
        self.entrada_puerto.grid(row=0, column=1, padx=(6, 14))
        tk.Label(fila, text="Semilla (opcional)", bg=TARJETA, fg=SUAVE).grid(row=0, column=2, sticky="w")
        self.entrada_semilla = ttk.Entry(fila, textvariable=self.var_semilla, width=12)
        self.entrada_semilla.grid(row=0, column=3, padx=(6, 14))
        self.check_entero = ttk.Checkbutton(fila, text="DHT11 (valores enteros)", variable=self.var_entero)
        self.check_entero.grid(row=0, column=4)
        botones = tk.Frame(marco, bg=TARJETA)
        botones.pack(fill="x", pady=(10, 0))
        self.boton_encender = ttk.Button(botones, text="Encender Arduino", style="Acento.TButton",
                                         command=self.alternar)
        self.boton_encender.pack(side="left")
        self.etiqueta_estado = tk.Label(botones, text="Apagado", bg=TARJETA, fg=SUAVE, font=(FUENTE, 10))
        self.etiqueta_estado.pack(side="left", padx=12)

        # --- Lectura actual
        marco = self._tarjeta("Lectura actual")
        valores = tk.Frame(marco, bg=TARJETA)
        valores.pack(fill="x")
        self.etiquetas_valor = {}
        for i, (campo, nombre, unidad) in enumerate((("temperatura", "Temperatura", "°C"),
                                                     ("humedad", "Humedad", "%"), ("gas", "Gas", ""))):
            valores.columnconfigure(i, weight=1, uniform="v")
            tk.Label(valores, text=nombre, bg=TARJETA, fg=SUAVE).grid(row=0, column=i, sticky="w")
            etiqueta = tk.Label(valores, text="—", bg=TARJETA, fg=TEXTO, font=(FUENTE, 20, "bold"))
            etiqueta.grid(row=1, column=i, sticky="w")
            self.etiquetas_valor[campo] = (etiqueta, unidad)
        self.boton_gas = ttk.Button(marco, text="Simular evento de gas", command=self.evento_gas,
                                    state="disabled")
        self.boton_gas.pack(anchor="w", pady=(10, 0))

        # --- Monitor serie
        marco = self._tarjeta("Monitor serie (lo que sale del Arduino)")
        self.monitor = tk.Text(marco, height=9, wrap="none", bg="#F7F9FA", fg=TEXTO, relief="flat",
                               font=("Consolas", 9), state="disabled")
        self.monitor.pack(fill="x")

        # --- Exportar
        marco = self._tarjeta("Exportar lo enviado a la aplicación")
        fila = tk.Frame(marco, bg=TARJETA)
        fila.pack(fill="x")
        ttk.Button(fila, text="Exportar CSV…", command=lambda: self.exportar("csv")).pack(side="left")
        ttk.Button(fila, text="Exportar JSON…", command=lambda: self.exportar("json")).pack(side="left", padx=8)
        self.etiqueta_registros = tk.Label(fila, text="0 registros", bg=TARJETA, fg=SUAVE)
        self.etiqueta_registros.pack(side="left", padx=8)
        tk.Label(marco, text="Cada archivo indica origen=simulado y la semilla usada.",
                 bg=TARJETA, fg=SUAVE, font=(FUENTE, 9)).pack(anchor="w", pady=(8, 0))
        self.etiqueta_exportado = tk.Label(marco, text="", bg=TARJETA, fg=EXITO, font=(FUENTE, 9),
                                           wraplength=560, justify="left")
        self.etiqueta_exportado.pack(anchor="w")

    # --- Encender / apagar ---------------------------------------------------------

    def alternar(self):
        if self.emisor is None:
            self.encender()
        else:
            self.apagar()

    # Propósito: validar la configuración, abrir el puerto y empezar a emitir una línea por segundo.
    def encender(self):
        texto_puerto, texto_semilla = self.var_puerto.get().strip(), self.var_semilla.get().strip()
        if not texto_puerto.isdigit() or not 1024 <= int(texto_puerto) < 65536:
            self._estado("⚠ Puerto inválido (use un número entre 1024 y 65535).", ERROR)
            return
        if texto_semilla and not texto_semilla.isdigit():
            self._estado("⚠ La semilla debe ser un número entero.", ERROR)
            return
        if self.registros and not self.exportado and not messagebox.askyesno(
                "Datos sin exportar", f"Hay {len(self.registros)} registros sin exportar.\n"
                "Si vuelve a encender se descartarán. ¿Continuar?"):
            return
        emisor = EmisorSerie(int(texto_puerto))
        try:
            emisor.iniciar()
        except OSError as error:
            self._estado(f"✖ No se pudo abrir el puerto {texto_puerto}: {error}", ERROR)
            return
        self.generador = GeneradorSensor(int(texto_semilla) if texto_semilla else None,
                                         dht_entero=self.var_entero.get())
        self.emisor = emisor
        self.registros, self.exportado = [], True
        self.meta = {"semilla": self.generador.semilla, "frecuencia_s": PERIODO_MS / 1000,
                     "resolucion_dht11": "entera" if self.var_entero.get() else "decimal",
                     "puerto_local": emisor.puerto}
        self._escribir_monitor(None)
        for control in (self.entrada_puerto, self.entrada_semilla, self.check_entero):
            control.configure(state="disabled")
        self.boton_encender.configure(text="Apagar Arduino", style="Peligro.TButton")
        self.boton_gas.configure(state="normal")
        self._tic()

    # Propósito: detener la emisión y cerrar el puerto. Los registros se conservan para exportarlos.
    def apagar(self):
        if self._id_tic is not None:
            self.after_cancel(self._id_tic)
            self._id_tic = None
        self.emisor.detener()
        self.emisor = None
        for control in (self.entrada_puerto, self.entrada_semilla, self.check_entero):
            control.configure(state="normal")
        self.boton_encender.configure(text="Encender Arduino", style="Acento.TButton")
        self.boton_gas.configure(state="disabled")
        self._estado("Apagado", SUAVE)

    def evento_gas(self):
        if self.generador is not None:
            self.generador.evento_gas()

    # Propósito: cada segundo, generar una lectura, mostrarla y enviarla (si hay aplicación conectada).
    # Solo lo que realmente se envía queda en 'registros' y se puede exportar.
    def _tic(self):
        lectura = self.generador.siguiente()
        linea = GeneradorSensor.linea(lectura)
        momento = datetime.now()
        enviado = self.emisor.enviar(linea)
        if enviado:
            self.registros.append(armar_registro(momento, lectura))
            self.exportado = False
        for campo, (etiqueta, unidad) in self.etiquetas_valor.items():
            valor = "—" if lectura is None else (f"{lectura[campo]:.1f}".replace(".", ",") if campo != "gas"
                                                  else str(lectura[campo]))
            etiqueta.configure(text=f"{valor} {unidad}".strip())
        self._escribir_monitor(f"{momento:%H:%M:%S}  {linea:<16} {'✔ enviado' if enviado else '· sin aplicación'}")
        self.etiqueta_registros.configure(text=f"{len(self.registros)} registros")
        if enviado:
            self._estado("● Enviando a la aplicación", EXITO)
        else:
            self._estado(f"Encendido en el puerto {self.emisor.puerto} · esperando a la aplicación…", AVISO)
        self._id_tic = self.after(PERIODO_MS, self._tic)

    def _estado(self, texto, color):
        self.etiqueta_estado.configure(text=texto, fg=color)

    # Propósito: añadir una línea al monitor (None = vaciarlo) y conservar solo las últimas.
    def _escribir_monitor(self, texto):
        self.monitor.configure(state="normal")
        if texto is None:
            self.monitor.delete("1.0", "end")
        else:
            self.monitor.insert("end", texto + "\n")
            if int(self.monitor.index("end-1c").split(".")[0]) > MAX_LINEAS_MONITOR:
                self.monitor.delete("1.0", "2.0")
            self.monitor.see("end")
        self.monitor.configure(state="disabled")

    # --- Exportar ----------------------------------------------------------------------

    # Propósito: guardar todo lo enviado a la aplicación en CSV o JSON.
    def exportar(self, formato):
        if not self.registros:
            messagebox.showinfo("Sin datos", "Todavía no se ha enviado nada a la aplicación.")
            return
        ruta = filedialog.asksaveasfilename(
            title=f"Exportar {formato.upper()}", defaultextension=f".{formato}",
            initialfile=f"simulador_{datetime.now():%Y%m%d_%H%M%S}.{formato}",
            filetypes=[(formato.upper(), f"*.{formato}"), ("Todos", "*.*")])
        if not ruta:
            return
        try:
            if formato == "csv":
                exportar_csv(ruta, self.registros)
            else:
                exportar_json(ruta, self.registros, self.meta)
        except OSError as error:
            messagebox.showerror("No se pudo guardar", str(error))
            return
        self.exportado = True
        self.etiqueta_exportado.configure(text=f"✔ {len(self.registros)} registros exportados a {ruta}")

    def _cerrar(self):
        if self.emisor is not None:
            self.apagar()
        self.destroy()


if __name__ == "__main__":
    VentanaSimulador().mainloop()
