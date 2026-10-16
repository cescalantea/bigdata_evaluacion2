"""
SERVIDOR DE PRUEBA (solo para el cliente del Grupo 3).

NO es el servidor oficial: ese lo construye el Grupo 4. Existe para comprobar,
desde nuestro lado, que los datos realmente llegan completos.

Sigue el mismo contrato del demo del Grupo 4:
    1. Lee bytes hasta que el cliente cierra su lado de la conexión.
    2. Los datos son un arreglo JSON de registros seguido de la marca <EOF>.
    3. Aquí además responde una línea JSON: {"ok": true, "filas": N} (opcional para ellos).

Comprobaciones de integridad:
    - La marca <EOF> está al final (si falta, el envío quedó cortado).
    - El JSON es válido y es una lista de objetos.
    - Se cuentan las filas y las columnas.

Tiene una ventana mínima con el interruptor "Iniciar escucha / Detener escucha".

Ejecutar:  python servidor_prueba.py
"""

import json
import os
import queue
import socket
import threading
import tkinter as tk
from tkinter import ttk

HOST = os.environ.get("SERVIDOR_HOST", "127.0.0.1")
PUERTO = int(os.environ.get("SERVIDOR_PUERTO", "12345"))
MARCA_FIN = b"<EOF>"
MAX_BYTES = 512 * 1024 * 1024   # tope de lo que aceptamos: protege la memoria
TIEMPO_ESPERA = 60


# ---------------------------------------------------------------------------
# Recepción (sin GUI): es lo que comprueba que los datos llegaron bien
# ---------------------------------------------------------------------------

# Propósito: leer todo lo que envíe el cliente, hasta que cierre la conexión.
# Se guardan BYTES y se decodifica al final: si decodificáramos cada trozo por separado,
# una tilde partida entre dos recv() rompería el texto.
def recibir_todo(conexion):
    fragmentos, total = [], 0
    while True:
        fragmento = conexion.recv(1024 * 1024)
        if not fragmento:
            return b"".join(fragmentos)
        total += len(fragmento)
        if total > MAX_BYTES:
            raise ValueError("El cliente superó el tamaño máximo permitido.")
        fragmentos.append(fragmento)


# Propósito: atender a UN cliente de principio a fin.
# 'registrar' es la función que recibe los mensajes para mostrarlos (por defecto, print).
# Devuelve un diccionario con el resultado (lo usan las pruebas automáticas).
def atender(conexion, registrar=print):
    conexion.settimeout(TIEMPO_ESPERA)
    crudo = recibir_todo(conexion)

    ok, detalle, registros = False, "", []
    if not crudo.endswith(MARCA_FIN):
        detalle = "Falta la marca <EOF>: el envío quedó incompleto."
    else:
        try:
            registros = json.loads(crudo[:-len(MARCA_FIN)].decode("utf-8"))
            if not isinstance(registros, list) or not all(isinstance(r, dict) for r in registros):
                detalle = "El JSON debe ser una lista de registros."
            else:
                ok = True
        except ValueError as error:
            detalle = f"JSON inválido: {error}"

    respuesta = {"ok": ok, "filas": len(registros) if ok else 0, "detalle": detalle or "Recepción correcta."}
    conexion.sendall(json.dumps(respuesta).encode("utf-8"))

    if ok:
        columnas = list(registros[0]) if registros else []
        registrar(f"✔ {len(registros):,} filas recibidas · {len(crudo) / 1e6:.1f} MB")
        registrar(f"   Columnas: {columnas}")
        for fila in registros[:2]:
            registrar(f"   {fila}")
    else:
        registrar(f"✖ {detalle}")
    return {"ok": ok, "registros": registros, "detalle": detalle}


# ---------------------------------------------------------------------------
# Ventana mínima con el interruptor de escucha
# ---------------------------------------------------------------------------

FONDO, TARJETA, TEXTO, SUAVE = "#EEF2F5", "#FFFFFF", "#1F2A37", "#6B7785"
ACENTO, ACENTO_OSCURO, EXITO, ERROR, AVISO = "#0E7490", "#155E75", "#15803D", "#B91C1C", "#B45309"
FUENTE = "Segoe UI"


class ServidorPrueba(tk.Tk):
    """
    Ventana del servidor de prueba. El botón alterna entre "Iniciar escucha" y
    "Detener escucha". Mientras escucha, un hilo acepta clientes de a uno.
    """

    def __init__(self):
        super().__init__()
        self.title("Servidor de prueba · Grupo 3")
        self.geometry("720x500")
        self.minsize(560, 380)
        self.configure(bg=FONDO)

        self.servidor = None              # socket que escucha (None = detenido)
        self.detener_escucha = threading.Event()   # se activa para pedirle al hilo que termine
        self.cola = queue.Queue()         # los hilos dejan aquí tareas para la ventana

        self._crear_estilos()
        self._crear_widgets()
        self.protocol("WM_DELETE_WINDOW", self._cerrar)
        self.after(100, self._revisar_cola)

    # Propósito: colores y tipografía (misma línea visual que el cliente).
    def _crear_estilos(self):
        estilo = ttk.Style(self)
        estilo.theme_use("clam")
        estilo.configure(".", font=(FUENTE, 10), background=TARJETA, foreground=TEXTO)
        estilo.configure("Tarjeta.TFrame", background=TARJETA)
        estilo.configure("Fondo.TFrame", background=FONDO)
        estilo.configure("Suave.TLabel", foreground=SUAVE, background=TARJETA)
        estilo.configure("TButton", padding=(14, 7), background="#E3E8ED", borderwidth=0,
                         font=(FUENTE, 10, "bold"))
        estilo.configure("Acento.TButton", background=ACENTO, foreground="white")
        estilo.map("Acento.TButton", background=[("active", ACENTO_OSCURO)])
        estilo.configure("Alto.TButton", background=ERROR, foreground="white")
        estilo.map("Alto.TButton", background=[("active", "#991B1B")])

    # Propósito: construir la pantalla: controles arriba y registro de mensajes abajo.
    def _crear_widgets(self):
        tk.Label(self, text="Servidor de prueba", bg=FONDO, fg=TEXTO,
                 font=(FUENTE, 18, "bold")).pack(anchor="w", padx=20, pady=(16, 0))
        tk.Label(self, text="Recibe lo que envía el cliente y comprueba que llegó completo",
                 bg=FONDO, fg=SUAVE, font=(FUENTE, 10)).pack(anchor="w", padx=20)

        panel = ttk.Frame(self, style="Tarjeta.TFrame", padding=14)
        panel.pack(fill="x", padx=20, pady=(12, 8))
        self.var_ip, self.var_puerto = tk.StringVar(value=HOST), tk.StringVar(value=str(PUERTO))
        ttk.Label(panel, text="IP", style="Suave.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Entry(panel, textvariable=self.var_ip, width=16).grid(row=0, column=1, padx=(6, 14))
        ttk.Label(panel, text="Puerto", style="Suave.TLabel").grid(row=0, column=2, sticky="w")
        ttk.Entry(panel, textvariable=self.var_puerto, width=7).grid(row=0, column=3, padx=(6, 14))
        self.boton = ttk.Button(panel, text="Iniciar escucha", style="Acento.TButton",
                                command=self.alternar)
        self.boton.grid(row=0, column=4, sticky="e")
        panel.columnconfigure(4, weight=1)
        self.estado = tk.Label(panel, text="● Detenido", bg=TARJETA, fg=SUAVE,
                               font=(FUENTE, 10, "bold"), anchor="w")
        self.estado.grid(row=1, column=0, columnspan=5, sticky="we", pady=(10, 0))

        contenedor = ttk.Frame(self, style="Tarjeta.TFrame", padding=10)
        contenedor.pack(fill="both", expand=True, padx=20, pady=(0, 16))
        self.registro = tk.Text(contenedor, wrap="word", relief="flat", bg="#F7F9FA", fg=TEXTO,
                                font=("Consolas", 9), padx=10, pady=8, state="disabled")
        scroll = ttk.Scrollbar(contenedor, orient="vertical", command=self.registro.yview)
        self.registro.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        self.registro.pack(side="left", fill="both", expand=True)

    # Propósito: cambiar el texto y color de la línea de estado.
    def _mostrar_estado(self, texto, color):
        self.estado.configure(text=texto, fg=color)

    # Propósito: agregar una línea al registro. Solo el hilo principal puede llamarla.
    def _escribir(self, texto):
        self.registro.configure(state="normal")
        self.registro.insert("end", texto + "\n")
        self.registro.see("end")
        self.registro.configure(state="disabled")

    # Propósito: lo usan los hilos para escribir en el registro de forma segura.
    def registrar(self, texto):
        self.cola.put(lambda: self._escribir(texto))

    # --- El interruptor -------------------------------------------------------

    # Propósito: el botón hace una cosa u otra según el estado actual.
    def alternar(self):
        if self.servidor is None:
            self.iniciar()
        else:
            self.detener()

    # Propósito: abrir el puerto y empezar a escuchar en un hilo aparte.
    # Si el puerto está ocupado, lo explica en pantalla en vez de cerrar el programa.
    def iniciar(self):
        host, texto_puerto = self.var_ip.get().strip(), self.var_puerto.get().strip()
        if not host or not texto_puerto.isdigit() or not 0 < int(texto_puerto) < 65536:
            self._mostrar_estado("● Revise la IP y el puerto (1-65535)", ERROR)
            return
        try:
            servidor = socket.create_server((host, int(texto_puerto)))
        except OSError as error:
            if error.errno in (10048, 98):  # Windows / Linux: dirección ya en uso
                motivo = "el puerto ya está en uso (¿hay otro servidor abierto?)"
            else:
                motivo = str(error)
            self._mostrar_estado(f"● No se pudo escuchar en {host}:{texto_puerto}: {motivo}", ERROR)
            return
        servidor.settimeout(0.5)  # así el hilo revisa periódicamente si lo mandaron detener
        self.servidor = servidor
        self.detener_escucha = threading.Event()
        threading.Thread(target=self._bucle, args=(servidor, self.detener_escucha), daemon=True).start()
        self.boton.configure(text="Detener escucha", style="Alto.TButton")
        self._mostrar_estado(f"● Escuchando en {host}:{texto_puerto}", EXITO)
        self.registrar(f"— Escucha iniciada en {host}:{texto_puerto} —")

    # Propósito: pedir que se deje de escuchar. El que cierra el socket es el propio hilo
    # (en Windows, cerrarlo desde otro hilo no lo libera mientras accept() espera).
    # Si hay una recepción en curso, se termina primero.
    def detener(self):
        if self.servidor is None or self.detener_escucha.is_set():
            return
        self.detener_escucha.set()
        self.boton.configure(state="disabled")
        self._mostrar_estado("● Deteniendo…", AVISO)

    # Propósito: (ventana) el hilo ya cerró el socket: dejar todo listo para iniciar de nuevo.
    def _al_detenerse(self):
        self.servidor = None
        self.boton.configure(text="Iniciar escucha", style="Acento.TButton", state="normal")
        self._mostrar_estado("● Detenido", SUAVE)
        self._escribir("— Escucha detenida —")

    # Propósito: (hilo) aceptar clientes uno tras otro hasta que pidan detener.
    # Al terminar SIEMPRE cierra el socket, para liberar el puerto.
    def _bucle(self, servidor, detener):
        try:
            while not detener.is_set():
                try:
                    conexion, direccion = servidor.accept()
                except socket.timeout:
                    continue  # nadie se conectó en 0,5 s: revisar si piden detener
                except OSError:
                    break
                self.registrar(f"Cliente conectado: {direccion[0]}")
                self.cola.put(lambda: self._mostrar_estado("● Recibiendo datos…", AVISO))
                with conexion:
                    try:
                        atender(conexion, self.registrar)
                    except (OSError, ValueError) as error:
                        self.registrar(f"✖ Transferencia interrumpida: {error}")
                if not detener.is_set():  # seguimos escuchando: volver al estado normal
                    self.cola.put(lambda: self._mostrar_estado(
                        f"● Escuchando en {self.var_ip.get()}:{self.var_puerto.get()}", EXITO))
        finally:
            servidor.close()
            self.cola.put(self._al_detenerse)

    # Propósito: ejecutar las tareas que dejaron los hilos (Tkinter solo se toca desde el principal).
    def _revisar_cola(self):
        try:
            while True:
                self.cola.get_nowait()()
        except queue.Empty:
            pass
        self.after(100, self._revisar_cola)

    # Propósito: al cerrar la ventana, pedir al hilo que termine (libera el puerto) y salir.
    def _cerrar(self):
        self.detener_escucha.set()
        self.destroy()


if __name__ == "__main__":
    ServidorPrueba().mainloop()
