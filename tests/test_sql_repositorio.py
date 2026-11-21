from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
import sql_repositorio as sql
import servidor_prueba as servidor


def datos():
    return pd.DataFrame([dict(fecha='2015-01-01', estacion_id=1,
        estacion_nombre='Rio', latitud='-33.1234', longitud='-70.1234',
        zona_latitudinal='Centro', area_cuenca_km2='467.5562216176950', caudal_m3_s='1.250'),
        dict(fecha='2015-01-02', estacion_id=1,
        estacion_nombre='Rio', latitud='-33.1234', longitud='-70.1234',
        zona_latitudinal='Centro', area_cuenca_km2='467.5562216176950', caudal_m3_s='2.250')])


@pytest.fixture
def repo(monkeypatch):
    monkeypatch.setattr(sql, 'load_dotenv', lambda: None)
    monkeypatch.setenv('DATABASE_URL', 'postgresql://user:secret@host/neondb?sslmode=require')
    return sql.SQLRepositorio()


def test_deshabilitado(repo, monkeypatch):
    monkeypatch.delenv('DATABASE_URL')
    r = sql.SQLRepositorio()
    assert not r.habilitado
    assert r.guardar_dataframe(pd.DataFrame())['insertados'] == 0


def test_tipos_separacion_precision(repo):
    estaciones, mediciones = repo.preparar(datos())
    assert len(estaciones) == 1 and len(mediciones) == 2
    assert estaciones[0][-1] == Decimal('467.5562216176950')
    assert mediciones[0] == (1, date(2015, 1, 1), Decimal('1.250'))
    assert len(repo.preparar(pd.concat([datos(), datos()]))[1]) == 2


@pytest.mark.parametrize('columna,valor', [('fecha','2015-02-30'), ('estacion_id',1.5),
    ('latitud',91), ('longitud',181), ('caudal_m3_s','1.2345'),
    ('area_cuenca_km2','NaN'), ('estacion_nombre',None), ('caudal_m3_s',-1)])
def test_invalidos_antes_conexion(repo, columna, valor):
    df = datos(); df.loc[0,columna] = valor
    repo.conectar = Mock()
    with pytest.raises(ValueError): repo.guardar_dataframe(df)
    repo.conectar.assert_not_called()


def test_columnas_e_inconsistencias(repo):
    assert repo.guardar_dataframe(datos().drop(columns='fecha'))['estado'] == 'omitido'
    df = datos(); df.loc[1,'estacion_nombre'] = 'Otro'
    with pytest.raises(ValueError, match='Metadatos'): repo.preparar(df)
    df = pd.concat([datos(), datos().assign(caudal_m3_s='3.000')])
    with pytest.raises(ValueError, match='contradictorias'): repo.preparar(df)


class BaseFalsa:
    def __init__(self, falla=False):
        self.falla = falla; self.estaciones = set(); self.mediciones = set()
        self.copias = []; self.commit = 0; self.rollback = 0; self.rowcount = 0
    def __enter__(self):
        self.pendientes_e = self.estaciones.copy(); self.pendientes_m = self.mediciones.copy()
        return self
    def __exit__(self, tipo, *args):
        if tipo: self.rollback += 1
        else:
            self.estaciones = self.pendientes_e; self.mediciones = self.pendientes_m
            self.commit += 1
    @contextmanager
    def cursor(self): yield self
    @contextmanager
    def copy(self, sentencia):
        filas = []; self.copias.append((sentencia, filas))
        yield SimpleNamespace(write_row=filas.append)
    def execute(self, sentencia):
        if sentencia.startswith('INSERT INTO public.estaciones'):
            filas = self.copias[-2][1]; antes = len(self.pendientes_e)
            self.pendientes_e.update(filas); self.rowcount = len(self.pendientes_e)-antes
        if sentencia.startswith('INSERT INTO public.mediciones'):
            if self.falla: raise RuntimeError('secret')
            filas = self.copias[-1][1]; antes = len(self.pendientes_m)
            self.pendientes_m.update(filas); self.rowcount = len(self.pendientes_m)-antes
    def fetchone(self): return None
    def close(self): pass


def test_copy_lotes_reenvio_commit(repo, monkeypatch):
    base = BaseFalsa(); repo.tamano_lote = 1
    monkeypatch.setattr(repo, 'conectar', lambda: base)
    primero = repo.guardar_dataframe(datos())
    segundo = repo.guardar_dataframe(datos())
    assert primero['insertados'] == 2 and primero['estaciones_insertadas'] == 1
    assert segundo['insertados'] == 0 and segundo['existentes'] == 2
    assert base.commit == 2 and len(base.mediciones) == 2
    assert [len(filas) for _,filas in base.copias] == [1,2,1,2]


def test_rollback(repo, monkeypatch, caplog):
    base = BaseFalsa(True)
    monkeypatch.setattr(repo, 'conectar', lambda: base)
    with pytest.raises(RuntimeError): repo.guardar_dataframe(datos())
    assert base.rollback == 1 and not base.estaciones and not base.mediciones
    assert 'secret' not in caplog.text


def test_conexion_ssl_y_fallo(repo, monkeypatch, caplog):
    import psycopg
    conectar = Mock(side_effect=Exception('secret'))
    monkeypatch.setattr(psycopg, 'connect', conectar)
    with pytest.raises(RuntimeError, match='conectar'): repo.conectar()
    assert conectar.call_args.kwargs['sslmode'] == 'require'
    assert conectar.call_args.kwargs['connect_timeout'] == 10
    assert 'secret' not in caplog.text


def test_fallo_mongo_no_impide_sql(repo):
    mongo = Mock(); mongo.guardar_dataframe.side_effect = RuntimeError('secret')
    postgres = Mock(); postgres.guardar_dataframe.return_value = dict(habilitado=True,insertados=2)
    instancia = SimpleNamespace(mongo=mongo,sql=postgres)
    resultado = servidor.ServidorBigData._persistir_dataframe(instancia, datos())
    assert resultado['MongoDB']['estado'] == 'fallido'
    assert resultado['PostgreSQL']['insertados'] == 2
    postgres.guardar_dataframe.assert_called_once()

# Reutiliza el socket real y la GUI de las pruebas existentes.
from test_servidor import ventana, iniciar, terminar, en_hilo, enviar_crudo
import json


def test_tcp_sql_ack_y_reenvio(ventana):
    base = BaseFalsa()
    repo = sql.SQLRepositorio()
    repo.url = 'postgresql://example/neondb'
    repo.conectar = lambda: base
    ventana.sql = repo
    puerto = iniciar(ventana)
    mensaje = json.dumps(datos().to_dict('records')).encode() + b'<EOF>'
    for esperado in (2, 0):
        ack = terminar(ventana, en_hilo(lambda: enviar_crudo(puerto, mensaje)))
        assert ack == {'ok': True, 'filas': 2}
        assert ventana._ultima_persistencia['PostgreSQL']['insertados'] == esperado
    assert len(base.estaciones) == 1 and len(base.mediciones) == 2


def test_tcp_fallo_sql_con_mongo_exitoso(ventana):
    ventana.mongo = Mock()
    ventana.mongo.guardar_dataframe.return_value = dict(habilitado=True, insertados=2)
    ventana.sql = Mock()
    ventana.sql.guardar_dataframe.side_effect = RuntimeError('secret')
    puerto = iniciar(ventana)
    mensaje = json.dumps(datos().to_dict('records')).encode() + b'<EOF>'
    ack = terminar(ventana, en_hilo(lambda: enviar_crudo(puerto, mensaje)))
    assert ack['ok'] is False and ack['filas'] == 2
    assert ack['persistencia']['MongoDB']['estado'] == 'guardado'
    assert ack['persistencia']['PostgreSQL']['estado'] == 'fallido'
    assert 'secret' not in str(ack)

@pytest.mark.skipif(not __import__('os').getenv('NEON_TEST_DATABASE_URL'),
                    reason='Neon real requiere NEON_TEST_DATABASE_URL; solo lectura')
def test_neon_real_solo_lectura(monkeypatch):
    import os
    monkeypatch.setenv('DATABASE_URL', os.environ['NEON_TEST_DATABASE_URL'])
    repo = sql.SQLRepositorio()
    with repo.conectar() as conexion:
        with conexion.cursor() as cursor:
            cursor.execute('SET TRANSACTION READ ONLY')
            cursor.execute('SELECT count(*) FROM public.estaciones')
            assert cursor.fetchone()[0] >= 0
            cursor.execute('SELECT count(*) FROM public.mediciones_caudal')
            assert cursor.fetchone()[0] >= 0
    repo.cerrar()
