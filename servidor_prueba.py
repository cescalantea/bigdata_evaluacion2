"""
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
import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd

# ---------------------------------------------------------------------------
# Configuración y Estilos Visuales (Homologados con el Cliente)
# ---------------------------------------------------------------------------

HOST = os.environ.get("SERVIDOR_HOST", "127.0.0.1")
PUERTO = int(os.environ.get("SERVIDOR_PUERTO", "12345"))
MARCA_FIN = "<EOF>"

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

        # Variables de Red y Concurrencia
        self.servidor = None
        self.detener_escucha = threading.Event()
        self.cola = queue.Queue()

        self._crear_estilos()
        self._crear_widgets()
        self.protocol("WM_DELETE_WINDOW", self._cerrar)
        self.after(100, self._revisar_cola)

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
        
        # ---> CORRECCIÓN: Creamos un sub-marco para agrupar los controles con grid <---
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
        
        # La etiqueta de estado queda fuera de la cuadrícula y usa pack
        self.estado = tk.Label(panel_red, text="● Servidor detenido", bg=TARJETA, fg=SUAVE, font=(FUENTE, 10, "bold"), anchor="w")
        self.estado.pack(anchor="w", pady=(10, 0))

        # SECCIÓN 2: Filtros de Vista
        panel_filtro = Tarjeta(cuerpo, "")
        panel_filtro.pack(fill="x", pady=(0, 12))
        
        ttk.Label(panel_filtro, text="Buscar texto:", style="Suave.TLabel").pack(side="left")
        self.entry_filtro = ttk.Entry(panel_filtro, width=40)
        self.entry_filtro.pack(side="left", padx=10)
        
        self.btn_filtrar = ttk.Button(panel_filtro, text="Aplicar Filtro", command=self.aplicar_filtro, state="disabled")
        self.btn_filtrar.pack(side="left", padx=5)
        self.btn_limpiar = ttk.Button(panel_filtro, text="Limpiar Filtro", command=self.limpiar_filtro, state="disabled")
        self.btn_limpiar.pack(side="left", padx=5)

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

    def alternar_servidor(self):
        if self.servidor is None:
            self.iniciar()
        else:
            self.detener()

    def iniciar(self):
        host, texto_puerto = self.var_ip.get().strip(), self.var_puerto.get().strip()
        if not host or not texto_puerto.isdigit() or not 0 < int(texto_puerto) < 65536:
            self._mostrar_estado("● Revise la IP y el puerto (1-65535)", ERROR)
            return
            
        try:
            serv_socket = socket.create_server((host, int(texto_puerto)))
        except OSError as error:
            self._mostrar_estado(f"● Error al abrir puerto {texto_puerto}: {error}", ERROR)
            return
            
        serv_socket.settimeout(0.5)
        self.servidor = serv_socket
        self.detener_escucha.clear()
        
        threading.Thread(target=self._bucle_escucha, args=(serv_socket,), daemon=True).start()
        
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
                
                self.cola.put(lambda: self._mostrar_estado(f"● Recibiendo paquete de {direccion[0]}...", AVISO))
                
                # Manejar al cliente en un hilo separado
                threading.Thread(target=self._atender_cliente, args=(conexion,), daemon=True).start()
                
        finally:
            servidor.close()
            self.cola.put(self._al_detenerse)

    def _atender_cliente(self, conexion):
        fragmentos = []
        try:
            conexion.settimeout(60)
            # Leer en chunks grandes para optimizar Big Data
            while True:
                fragmento = conexion.recv(1024 * 1024).decode('utf-8')
                if not fragmento:
                    break
                fragmentos.append(fragmento)
                if MARCA_FIN in fragmento:
                    break

            datos_completos = "".join(fragmentos)
            
            if MARCA_FIN in datos_completos:
                datos_completos = datos_completos.replace(MARCA_FIN, "")
            
            self.cola.put(lambda: self._mostrar_estado("● Procesando JSON en Pandas...", ACENTO))
            
            # Cargar a Pandas
            lista_datos = json.loads(datos_completos)
            df_nuevo = pd.DataFrame(lista_datos)
            
            # Enviar confirmación al Cliente (Contrato de Datos)
            respuesta = {"ok": True, "filas": len(df_nuevo)}
            conexion.sendall(json.dumps(respuesta).encode("utf-8"))
            
            # Pasar los datos a la GUI
            self.cola.put(lambda: self._datos_recibidos(df_nuevo))
            
        except Exception as e:
            self.cola.put(lambda: self._mostrar_estado(f"● Error en transferencia: {e}", ERROR))
        finally:
            conexion.close()

    def _datos_recibidos(self, df_nuevo):
        self.df_recibido = df_nuevo
        self.df_actual = self.df_recibido
        self.pagina_actual = 0
        
        self.cargar_datos_en_tabla()
        
        self._mostrar_estado(f"● ¡Datos listos! {miles(len(self.df_recibido))} registros en memoria.", EXITO)
        self.btn_filtrar.configure(state="normal")
        self.btn_limpiar.configure(state="normal")

    # -----------------------------------------------------------------------
    # LÓGICA DE INTERFAZ, PAGINACIÓN Y FILTROS
    # -----------------------------------------------------------------------

    def cargar_datos_en_tabla(self):
        if self.df_actual is None or self.df_actual.empty:
            self.tabla.delete(*self.tabla.get_children())
            self.lbl_pagina.config(text="Sin resultados.")
            return

        self.tabla.delete(*self.tabla.get_children())
        
        columnas = list(self.df_actual.columns)
        self.tabla["columns"] = columnas
        
        for col in columnas:
            self.tabla.heading(col, text=col)
            self.tabla.column(col, width=120, minwidth=80, anchor="center")
            
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
        self.pagina_actual += 1
        self.cargar_datos_en_tabla()

    def pagina_anterior(self):
        if self.pagina_actual > 0:
            self.pagina_actual -= 1
            self.cargar_datos_en_tabla()

    def aplicar_filtro(self):
        texto = self.entry_filtro.get().lower().strip()
        if not texto:
            return
            
        self._mostrar_estado("● Filtrando datos...", AVISO)
        self.update_idletasks() # Forzar refresco visual
        
        # Filtro global en pandas convertido a string
        mask = self.df_recibido.astype(str).apply(lambda x: x.str.lower().str.contains(texto)).any(axis=1)
        self.df_actual = self.df_recibido[mask]
        self.pagina_actual = 0
        
        self.cargar_datos_en_tabla()
        self._mostrar_estado(f"● Filtro aplicado: {miles(len(self.df_actual))} coincidencias.", EXITO)

    def limpiar_filtro(self):
        self.entry_filtro.delete(0, tk.END)
        self.df_actual = self.df_recibido
        self.pagina_actual = 0
        self.cargar_datos_en_tabla()
        self._mostrar_estado(f"● Mostrando dataset completo: {miles(len(self.df_actual))} registros.", EXITO)

    def _revisar_cola(self):
        try:
            while True:
                self.cola.get_nowait()()
        except queue.Empty:
            pass
        self.after(100, self._revisar_cola)

    def _cerrar(self):
        self.detener_escucha.set()
        self.destroy()

if __name__ == "__main__":
    ServidorBigData().mainloop()