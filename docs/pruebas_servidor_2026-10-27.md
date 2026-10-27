# Revisión del servidor del Grupo 4 contra el cliente (27-10-2026)

Script revisado: `servidor_prueba.py` entregado manualmente el 27-10-2026
(cambio principal: el ACK se envía inmediatamente, antes de que la interfaz aplique los datos).

## Prueba extremo a extremo con el CSV real

| Dato | Valor |
|---|---|
| Archivo | `CAMELS_CL_caudales_2015_2019_569250.csv` (569.250 filas) |
| Tamaño de lote | 5.000 |
| Respuesta del servidor | `{"ok": true, "filas": 569250}` |
| Demora medida por el cliente | 9,4 s (7,6 s en la prueba del usuario) |
| Demora medida por el servidor | 9,4 s (coincide con el cliente) |
| Tabla del servidor poblada | a los 9,5 s |

Las diferencias entre 7,6 s y 9,4 s se deben a la máquina y al momento de la prueba.

## Pruebas automáticas: 5 fallan (antes fallaba 1)

Causadas por el ACK inmediato (el ACK ya no espera a que la interfaz acepte el dataset):

1. `test_cliente_real_consecutivas_integridad` y `test_carga_masiva_sintetica`: tras el ACK, `df_recibido` aún es `None`.
2. `test_no_ack_positivo_si_dashboard_rechaza` y `test_aceptacion_vencida_no_aplica_despues`: esperan ACK negativo si el Dashboard rechaza o la aceptación vence, pero llega `ok: true`.

Ya fallaba antes del cambio:

3. `test_dataset_real_si_disponible`: el límite de 8 s de la prueba es menor que los ~9 s del envío real. No es un error del servidor.

## Decisión pendiente (Grupo 4)

- Mantener el ACK rápido: actualizar las pruebas 1 y 2 para esperar a que la interfaz aplique los datos y replantear las de ACK negativo. Un cliente puede ver "recibido" aunque la interfaz luego rechace los datos.
- Volver al ACK posterior a la aceptación: se conserva "ACK = recibido y aceptado".

El contrato no cambia: un arreglo JSON, `<EOF>` y respuesta `{"ok", "filas"}`.

Estado: nota enviada al Grupo 4 el 27-10-2026. El script modificado no se incluye en esta rama.
