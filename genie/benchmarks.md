# Benchmarks del Genie Agent

Preguntas con respuesta conocida para medir la precisión de Genie. Se corren en modo **Chat**
(Genie compara su resultado con el SQL Answer). Volver a correrlas después de cada cambio en
instrucciones o consultas de ejemplo.

Último resultado: **6 de 6 Good** (2026-11-06).

| # | Pregunta | SQL Answer | Valor esperado | Evaluation note |
| --- | --- | --- | --- | --- |
| 1 | ¿Cuánto se ha recaudado en total? | `SELECT MEASURE(`Valor recaudado`) FROM recaudo_dev.gold.metricas_recaudo` | 2.083.629.150.947 | Flujo: suma todos los periodos, sin filtrar años |
| 2 | ¿Cuál es la brecha de recaudo? | `SELECT MEASURE(`Brecha de recaudo`) FROM recaudo_dev.gold.metricas_recaudo` | 22.716.312.302 | Total de todos los periodos, sin filtrar ningún mes. Incorrecto: filtrar un mes, o calcular esperado − recaudado sin la medida (~90.600 millones) |
| 3 | ¿Qué porcentaje de aportantes estaba en mora en diciembre de 2025? | `SELECT MEASURE(`Porcentaje de aportantes en mora`) FROM recaudo_dev.gold.metricas_cartera WHERE Periodo = '2025-12'` | 0,21797 | Si lo devuelve como 21,8 la lógica es correcta: revisar a mano |
| 4 | ¿Cuántas empresas tienen riesgo alto o crítico? | `SELECT MEASURE(`Aportantes riesgo alto o critico`) FROM recaudo_dev.gold.metricas_riesgo WHERE `Version del score` = 'ml-v1' AND Periodo = '2025-12'` | 1.938 | Incorrecto: 2.320 (mezcla ml-v1 y reglas-v1) o una cifra mayor (suma meses) |
| 5 | ¿Cuál es la tasa de éxito de las visitas? | `SELECT MEASURE(`Tasa de exito`) FROM recaudo_dev.gold.metricas_cobranza WHERE `Tipo de gestion` = 'Visita'` | 0,40550 | Sobre 43.356 visitas, todos los periodos |
| 6 | ¿Cuál es el saldo vencido total? (trampa) | `SELECT MEASURE(`Saldo vencido total`) FROM recaudo_dev.gold.metricas_cartera WHERE Periodo = '2025-12'` | 24.716.605.519 | Foto: no se suma entre meses. Incorrecto: 740.138.487.214 (suma de todos los meses) |
