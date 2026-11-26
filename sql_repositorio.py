"""
Módulo de persistencia relacional para almacenar los datos CAMELS recibidos
por el servidor Big Data en PostgreSQL alojado en Neon.
Autor: Grupo 4.
Licencia: solo educacional.
Fecha creación: 21-11-2026.
Fecha modificación: 21-11-2026.
Modificaciones: incorporación de SQLRepositorio con conexión segura mediante
SSL, configuración por variables de entorno, validación del DataFrame,
carga masiva mediante COPY y control de transacciones y duplicados.

PERSISTENCIA POSTGRESQL NEON (Grupo 4) - Repositorio de Datos CAMELS.

Reutiliza el DataFrame recibido por el servidor TCP y separa sus registros
en estaciones y mediciones de caudal para almacenarlos en las tablas
relacionales existentes, conservando la persistencia en MongoDB Atlas.
Valida columnas, fechas, tipos, precisión numérica y consistencia de los
datos antes de escribir. Inserta primero las estaciones y después las
mediciones dentro de una transacción, utilizando ON CONFLICT para evitar
duplicados idénticos y rollback ante errores previos a la confirmación.
Si DATABASE_URL no está configurada, la persistencia SQL queda deshabilitada.
"""

import logging
import os
import threading
from datetime import date
from decimal import Decimal, InvalidOperation
from urllib.parse import urlsplit, parse_qs

from dotenv import load_dotenv

COLUMNAS = ('fecha', 'estacion_id', 'estacion_nombre', 'latitud', 'longitud',
            'zona_latitudinal', 'area_cuenca_km2', 'caudal_m3_s')


class EsquemaIncompleto(ValueError):
    pass


class SQLRepositorio:
    def __init__(self):
        load_dotenv()
        self.url = os.getenv('DATABASE_URL', '').strip()
        self.tamano_lote = int(os.getenv('POSTGRES_BATCH_SIZE', '270000'))
        # Se conservan estos valores de respaldo por compatibilidad; la
        # configuracion recomendada queda definida en el archivo .env.
        self.timeout = int(os.getenv('POSTGRES_CONNECT_TIMEOUT', '10'))
        self.statement_timeout = int(os.getenv('POSTGRES_STATEMENT_TIMEOUT_MS', '45000'))
        if min(self.tamano_lote, self.timeout, self.statement_timeout) <= 0:
            raise ValueError('Los limites PostgreSQL deben ser positivos.')
        if self.url:
            uri = urlsplit(self.url)
            if uri.scheme not in ('postgresql', 'postgres') or not uri.hostname:
                raise ValueError('DATABASE_URL debe ser una URL PostgreSQL valida.')
            host_local = uri.hostname.lower() in ('localhost', '127.0.0.1', '::1')
            modo_predeterminado = 'disable' if host_local else 'require'
            modo = parse_qs(uri.query).get('sslmode', [modo_predeterminado])[-1]
            if modo not in ('disable', 'require', 'verify-ca', 'verify-full'):
                raise ValueError('sslmode de PostgreSQL no es valido.')
            if modo == 'disable' and not host_local:
                raise ValueError('sslmode=disable solo se permite para PostgreSQL local.')
            self.sslmode = modo
        self._conexion = None
        self._lock = threading.Lock()

    @property
    def habilitado(self):
        return bool(self.url)

    def conectar(self):
        if not self.habilitado:
            return None
        import psycopg
        try:
            self._conexion = psycopg.connect(
                self.url, sslmode=self.sslmode, connect_timeout=self.timeout,
                options=f'-c statement_timeout={self.statement_timeout} -c lock_timeout={self.statement_timeout}')
            return self._conexion
        except Exception:
            logging.error('PostgreSQL: fallo de conexion (detalle omitido para proteger credenciales).')
            raise RuntimeError('No se pudo conectar a PostgreSQL.') from None

    @staticmethod
    def _numero(valor, precision, escala, nombre):
        try:
            if isinstance(valor, bool):
                raise ValueError()
            numero = Decimal(str(valor))
            if not numero.is_finite() or abs(numero) >= Decimal(10) ** (precision - escala):
                raise ValueError()
            if numero != numero.quantize(Decimal(1).scaleb(-escala)):
                raise ValueError()
            return numero
        except (InvalidOperation, ValueError):
            raise ValueError(f'{nombre}: numero fuera de rango o precision SQL.') from None

    def preparar(self, dataframe):
        if not dataframe.columns.is_unique or set(dataframe.columns) != set(COLUMNAS):
            raise EsquemaIncompleto('SQL requiere exactamente las ocho columnas CAMELS: ' + ', '.join(COLUMNAS))
        estaciones, mediciones = {}, {}
        for fila in dataframe.loc[:, list(COLUMNAS)].itertuples(index=False, name=None):
            fecha, identificador, nombre, lat, lon, zona, area, caudal = fila
            try:
                fecha = date.fromisoformat(str(fecha))
            except (ValueError, TypeError):
                raise ValueError('fecha debe usar YYYY-MM-DD y ser una fecha valida.') from None
            identificador = self._numero(identificador, 10, 0, 'estacion_id')
            if not -2147483648 <= identificador <= 2147483647:
                raise ValueError('estacion_id fuera de rango INTEGER.')
            identificador = int(identificador)
            for texto, limite in ((nombre, 150), (zona, 50)):
                if not isinstance(texto, str) or not texto.strip() or len(texto) > limite or '\x00' in texto:
                    raise ValueError('Nombre o zona invalido.')
            lat = self._numero(lat, 8, 4, 'latitud')
            lon = self._numero(lon, 8, 4, 'longitud')
            area = self._numero(area, 20, 13, 'area_cuenca_km2')
            caudal = self._numero(caudal, 10, 3, 'caudal_m3_s')
            if not -90 <= lat <= 90 or not -180 <= lon <= 180 or area < 0 or caudal < 0:
                raise ValueError('Coordenadas, area o caudal fuera de rango.')
            estacion = (identificador, nombre, lat, lon, zona, area)
            if identificador in estaciones and estaciones[identificador] != estacion:
                raise ValueError('Metadatos inconsistentes para una estacion.')
            estaciones[identificador] = estacion
            clave = (identificador, fecha)
            if clave in mediciones and mediciones[clave] != caudal:
                raise ValueError('Mediciones contradictorias para estacion y fecha.')
            mediciones[clave] = caudal
        return list(estaciones.values()), [(i, f, c) for (i, f), c in mediciones.items()]

    def guardar_dataframe(self, dataframe):
        resultado = dict(habilitado=self.habilitado, insertados=0, estaciones_insertadas=0)
        if not self.habilitado:
            return dict(resultado, estado='deshabilitado')
        try:
            estaciones, mediciones = self.preparar(dataframe)
        except EsquemaIncompleto as error:
            logging.warning('PostgreSQL omitido: %s', error)
            return dict(resultado, estado='omitido', detalle=str(error))
        with self._lock:
            try:
                with self.conectar() as conexion:
                    with conexion.cursor() as cursor:
                        # Solo tablas temporales: nunca se altera el esquema persistente.
                        cursor.execute('CREATE TEMP TABLE carga_estaciones (LIKE public.estaciones INCLUDING DEFAULTS) ON COMMIT DROP')
                        cursor.execute('CREATE TEMP TABLE carga_mediciones (estacion_id INTEGER, fecha DATE, caudal_m3_s NUMERIC(10,3)) ON COMMIT DROP')
                        for tabla, filas in (('carga_estaciones (estacion_id,estacion_nombre,latitud,longitud,zona_latitudinal,area_cuenca_km2)', estaciones), ('carga_mediciones', mediciones)):
                            with cursor.copy(f'COPY {tabla} FROM STDIN') as copia:
                                for inicio in range(0, len(filas), self.tamano_lote):
                                    for fila in filas[inicio:inicio + self.tamano_lote]:
                                        copia.write_row(fila)
                        # Serializa las cargas de este repositorio antes de comprobar conflictos.
                        cursor.execute('SELECT pg_advisory_xact_lock(470412346)')
                        cursor.execute('SELECT 1 FROM public.estaciones e JOIN carga_estaciones c USING (estacion_id) WHERE ROW(e.estacion_nombre,e.latitud,e.longitud,e.zona_latitudinal,e.area_cuenca_km2) IS DISTINCT FROM ROW(c.estacion_nombre,c.latitud,c.longitud,c.zona_latitudinal,c.area_cuenca_km2) LIMIT 1')
                        if cursor.fetchone():
                            raise ValueError('Metadatos distintos de la estacion existente.')
                        cursor.execute('SELECT 1 FROM public.mediciones_caudal m JOIN carga_mediciones c USING (estacion_id,fecha) WHERE m.caudal_m3_s IS DISTINCT FROM c.caudal_m3_s LIMIT 1')
                        if cursor.fetchone():
                            raise ValueError('Caudal distinto de la medicion existente.')
                        cursor.execute('INSERT INTO public.estaciones (estacion_id,estacion_nombre,latitud,longitud,zona_latitudinal,area_cuenca_km2) SELECT estacion_id,estacion_nombre,latitud,longitud,zona_latitudinal,area_cuenca_km2 FROM carga_estaciones ON CONFLICT (estacion_id) DO NOTHING')
                        resultado['estaciones_insertadas'] = cursor.rowcount
                        cursor.execute('INSERT INTO public.mediciones_caudal (estacion_id,fecha,caudal_m3_s) SELECT * FROM carga_mediciones ON CONFLICT (estacion_id,fecha) DO NOTHING')
                        resultado['insertados'] = cursor.rowcount
                return dict(resultado, estado='guardado', procesados=len(dataframe), existentes=len(mediciones)-resultado['insertados'])
            except Exception:
                logging.error('PostgreSQL: escritura no confirmada; transaccion revertida si no hubo COMMIT. Detalle sensible omitido.')
                raise RuntimeError('PostgreSQL no confirmo la carga; verifique los conteos antes de reintentar.') from None
            finally:
                self.cerrar()

    def cerrar(self):
        if self._conexion is not None:
            self._conexion.close()
            self._conexion = None
