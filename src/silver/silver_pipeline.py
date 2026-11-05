"""Silver: tipos, calidad y cuarentena — Lakeflow Spark Declarative Pipelines.

Este archivo no se ejecuta como un notebook normal: es el código fuente de un
pipeline declarativo. Cada función decorada DECLARA una tabla; Databricks lee
todas las declaraciones, arma el grafo de dependencias y decide el orden.

Qué hace Silver:
  1. Convierte cada columna de texto (Bronze) a su tipo real con try_cast:
     un valor imposible queda en NULL en vez de romper la carga.
  2. Aplica reglas de calidad (expectations). Las filas que no cumplen salen
     de la tabla limpia y van a una tabla <nombre>_cuarentena con la lista de
     reglas que fallaron, para revisarlas sin perderlas.
  3. Construye el historial de versiones de los aportantes (SCD tipo 2) con
     AUTO CDC.

Configuración del pipeline (Settings -> Configuration):
  catalogo = recaudo_dev      (catálogo donde está el esquema bronze)
Catálogo y esquema por defecto del pipeline: recaudo_dev / silver.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOGO = spark.conf.get("catalogo", "recaudo_dev")
BRONZE = f"{CATALOGO}.bronze"
PERIODO_INICIO = "2023-01"            # pagos de periodos anteriores = cartera heredada

ESTADOS_PAGO = "'Oportuno','Tardío','Parcial','No pagado','Pendiente'"
TRAMOS = "'Sin Mora','1-30','31-60','61-90','>90'"
AUXILIARES = ["_ap_ok", "_cot_ok", "_rescued_data"]   # solo sirven para validar


# =============================================================================
# Utilidades
# =============================================================================
def tipar(df, tipos: dict):
    """Convierte columnas de texto a su tipo. try_cast deja NULL si no se puede."""
    for col, tipo in tipos.items():
        df = df.withColumn(col, F.expr(f"try_cast(`{col}` AS {tipo})"))
    return df


def seguro(regla: str) -> str:
    """Una regla que evalúa NULL cuenta como falla (no como 'sin dato')."""
    return f"coalesce(({regla}), false)"


def con_cuarentena(nombre: str, comentario: str, origen, reglas: dict, tipo: str = "stream"):
    """Declara dos tablas a partir del mismo origen:

    - <nombre>: solo las filas que cumplen TODAS las reglas (expect_all_or_drop).
      Las métricas de cada regla quedan en la pestaña Data quality del pipeline.
    - <nombre>_cuarentena: solo las filas que fallan alguna, con la columna
      _reglas_falladas para saber por qué.

    tipo = "stream" crea streaming tables (incrementales: solo procesan filas
    nuevas); tipo = "mv" crea materialized views (se recalculan completas).
    """
    reglas = {k: seguro(v) for k, v in reglas.items()}
    todas = " AND ".join(reglas.values())
    decorador = dp.table if tipo == "stream" else dp.materialized_view
    lector = (lambda t: spark.readStream.table(t)) if tipo == "stream" else (lambda t: spark.read.table(t))

    # Las reglas se evalúan en una vista temporal; la tabla final lee de ella y
    # quita las columnas auxiliares que solo servían para validar.
    @dp.temporary_view(name=f"{nombre}_validado")
    @dp.expect_all_or_drop(reglas)
    def _validado():
        return origen()

    @decorador(name=nombre, comment=comentario)
    def _limpia():
        df = lector(f"{nombre}_validado")
        return df.drop(*[c for c in AUXILIARES if c in df.columns])

    @decorador(name=f"{nombre}_cuarentena",
               comment=f"Filas de {nombre} que fallaron alguna regla de calidad.")
    def _cuarentena():
        fallas = F.filter(
            F.array(*[F.when(~F.expr(r), F.lit(n)) for n, r in reglas.items()]),
            lambda x: x.isNotNull())
        return (origen()
                .where(f"NOT ({todas})")
                .withColumn("_reglas_falladas", fallas)
                .withColumn("_fecha_cuarentena", F.current_timestamp()))


def aportantes_validos():
    """Lista estática de AportanteID que existen en Silver (para validar llaves foráneas)."""
    return (spark.read.table("aportante")
            .select(F.col("AportanteID").alias("_ap_ok")).distinct())


def con_fk_aportante(df):
    """Marca si el AportanteID de cada fila existe en la dimensión."""
    return df.join(F.broadcast(aportantes_validos()),
                   df["AportanteID"] == F.col("_ap_ok"), "left")


def leer_bronze(tabla: str, streaming: bool = True):
    lector = spark.readStream if streaming else spark.read
    return lector.table(f"{BRONZE}.{tabla}").drop("_ingest_ts")


# =============================================================================
# Aportante: SCD tipo 2 con AUTO CDC
# =============================================================================
# Dígito de verificación del NIT (algoritmo DIAN) escrito en SQL, para comparar
# con el que trae el archivo.
_PESOS = [41, 37, 29, 23, 19, 17, 13, 7, 3]
_SUMA = " + ".join(f"cast(substring(Nit, {i + 1}, 1) AS INT) * {w}" for i, w in enumerate(_PESOS))
DV_CALCULADO = f"CASE WHEN ({_SUMA}) % 11 IN (0, 1) THEN ({_SUMA}) % 11 ELSE 11 - ({_SUMA}) % 11 END"

REGLAS_APORTANTE = {
    "aportante_id_valido": "AportanteID RLIKE '^APT-[0-9]{6}$'",
    "nit_valido": "Nit RLIKE '^[89][0-9]{8}$'",
    "fecha_actualizacion_valida": "FechaActualizacion IS NOT NULL",
    "estado_valido": "EstadoAportante IN ('Activo','Inactivo','En liquidación')",
    "sin_datos_rescatados": "_rescued_data IS NULL",
}


def _aportante_tipado():
    df = tipar(leer_bronze("aportante"), {
        "DigitoVerificacion": "INT", "GeografiaID": "INT",
        "FechaAfiliacion": "DATE", "FechaActualizacion": "DATE",
    })
    return (df
            # Normalización: sin espacios sobrantes y en mayúsculas, el estándar
            # habitual para razones sociales en registros oficiales.
            .withColumn("RazonSocial", F.upper(F.trim(F.regexp_replace("RazonSocial", r"\s+", " "))))
            # Un DV mal digitado no invalida a la empresa: se marca y se conserva.
            .withColumn("DvValido", F.expr(f"DigitoVerificacion = {DV_CALCULADO}"))
            # RiesgoLatente es la "respuesta" oculta del generador: nunca pasa de Bronze.
            .drop("RiesgoLatente"))


@dp.temporary_view(name="aportante_cambios",
                   comment="Versiones de aportantes tipadas y validadas, listas para AUTO CDC.")
@dp.expect("dv_coincide", "DvValido")              # warn: se mide, no se descarta
@dp.expect_all_or_drop({k: seguro(v) for k, v in REGLAS_APORTANTE.items()})
def aportante_cambios():
    return _aportante_tipado()


@dp.table(name="aportante_cuarentena",
          comment="Versiones de aportantes que fallaron alguna regla de calidad.")
def aportante_cuarentena():
    reglas = {k: seguro(v) for k, v in REGLAS_APORTANTE.items()}
    fallas = F.filter(F.array(*[F.when(~F.expr(r), F.lit(n)) for n, r in reglas.items()]),
                      lambda x: x.isNotNull())
    return (_aportante_tipado()
            .where(f"NOT ({' AND '.join(reglas.values())})")
            .withColumn("_reglas_falladas", fallas))


# AUTO CDC compara cada versión con la anterior del mismo AportanteID, ordenando
# por FechaActualizacion, y mantiene el historial: cada versión tiene __START_AT
# (desde cuándo rige) y __END_AT (hasta cuándo; NULL = versión vigente).
dp.create_streaming_table(
    name="aportante",
    comment="Aportantes con historial de versiones (SCD tipo 2).")

dp.create_auto_cdc_flow(
    target="aportante",
    source="aportante_cambios",
    keys=["AportanteID"],
    sequence_by=F.col("FechaActualizacion"),
    stored_as_scd_type=2,
    except_column_list=["_rescued_data", "_source_file"],
)


@dp.materialized_view(name="aportante_actual",
                      comment="Versión vigente de cada aportante (una fila por empresa).")
def aportante_actual():
    return spark.read.table("aportante").where("__END_AT IS NULL")


# =============================================================================
# Dimensiones pequeñas: materialized views
# =============================================================================
con_cuarentena(
    "geografia", "Municipios con su regional.",
    lambda: tipar(leer_bronze("geografia", streaming=False), {"GeografiaID": "INT"}),
    {"geografia_id_valido": "GeografiaID IS NOT NULL",
     "dane_valido": "CodigoDANE RLIKE '^[0-9]{5}$'"},
    tipo="mv")

con_cuarentena(
    "tiempo", "Calendario diario con festivos de Colombia.",
    lambda: tipar(leer_bronze("tiempo", streaming=False), {
        "Fecha": "DATE", "FechaKey": "INT", "Anio": "INT", "Mes": "INT", "Trimestre": "INT",
        "Semana": "INT", "Dia": "INT", "DiaSemana": "INT",
        "EsFestivo": "BOOLEAN", "EsDiaHabil": "BOOLEAN"}),
    {"fecha_valida": "Fecha IS NOT NULL"},
    tipo="mv")

con_cuarentena(
    "cotizante", "Trabajadores afiliados, con su empleador.",
    lambda: con_fk_aportante(tipar(leer_bronze("cotizante", streaming=False), {
        "FechaNacimiento": "DATE", "Edad": "INT", "Salario": "DECIMAL(18,2)",
        "FechaIngreso": "DATE", "FechaRetiro": "DATE"})),
    {"cotizante_id_valido": "CotizanteID RLIKE '^COT-[0-9]{7}$'",
     "aportante_existe": "_ap_ok IS NOT NULL",
     "edad_valida": "Edad BETWEEN 18 AND 70",
     "salario_positivo": "Salario > 0",
     "fechas_coherentes": "FechaRetiro IS NULL OR FechaRetiro >= FechaIngreso",
     "sin_datos_rescatados": "_rescued_data IS NULL"},
    tipo="mv")


# =============================================================================
# Hechos: streaming tables (incrementales)
# =============================================================================
def _cotizantes_validos():
    return spark.read.table("cotizante").select(F.col("CotizanteID").alias("_cot_ok"))


con_cuarentena(
    "aporte_detalle", "Aporte esperado por cotizante y periodo (IBC, cotización, FSP).",
    lambda: (tipar(leer_bronze("aporte_detalle"), {
                "DiasCotizados": "INT", "IBC": "DECIMAL(18,2)", "TarifaPension": "DECIMAL(5,4)",
                "ValorCotizacion": "DECIMAL(18,2)", "ValorFSP": "DECIMAL(18,2)",
                "ValorEsperado": "DECIMAL(18,2)"})
             .join(F.broadcast(_cotizantes_validos()), F.col("CotizanteID") == F.col("_cot_ok"), "left")),
    {"periodo_valido": "Periodo RLIKE '^20[0-9]{2}-(0[1-9]|1[0-2])$'",
     "cotizante_existe": "_cot_ok IS NOT NULL",
     "dias_validos": "DiasCotizados BETWEEN 1 AND 30",
     "ibc_positivo": "IBC > 0",
     "valor_cuadra": "ValorEsperado = ValorCotizacion + ValorFSP",
     "sin_datos_rescatados": "_rescued_data IS NULL"})

con_cuarentena(
    "aporte", "Aporte esperado contra pagado por aportante y periodo, al corte.",
    lambda: con_fk_aportante(tipar(leer_bronze("aporte"), {
        "AporteID": "BIGINT", "NumeroCotizantes": "INT", "ValorEsperado": "DECIMAL(18,2)",
        "ValorPagado": "DECIMAL(18,2)", "FechaLimitePago": "DATE", "FechaPago": "DATE",
        "DiasRetraso": "INT"})),
    {"aportante_existe": "_ap_ok IS NOT NULL",
     "pagado_no_negativo": "ValorPagado >= 0",
     "pagado_no_supera_esperado": "ValorPagado <= ValorEsperado",
     "estado_valido": f"EstadoPago IN ({ESTADOS_PAGO})",
     "sin_datos_rescatados": "_rescued_data IS NULL"})


def _pagos():
    df = tipar(leer_bronze("pago"), {
        "PagoID": "BIGINT", "FechaPago": "DATE", "ValorCapital": "DECIMAL(18,2)",
        "ValorInteres": "DECIMAL(18,2)", "ValorPagado": "DECIMAL(18,2)", "AcuerdoID": "BIGINT"})
    return (con_fk_aportante(df)
            # Un mismo PagoID repetido es un duplicado técnico: se conserva uno.
            .dropDuplicates(["PagoID"])
            # Pagos de periodos de 2022: cartera heredada, sin fila en 'aporte'.
            .withColumn("EsCarteraHeredada", F.col("PeriodoAplicado") < F.lit(PERIODO_INICIO)))


con_cuarentena(
    "pago", "Transacciones de pago aplicadas a un periodo.",
    _pagos,
    {"aportante_existe": "_ap_ok IS NOT NULL",
     "valor_positivo": "ValorPagado > 0",
     "valor_cuadra": "ValorPagado = ValorCapital + ValorInteres",
     # Un pago no puede ocurrir antes de que empiece el periodo que paga.
     "fecha_posible": "FechaPago >= to_date(concat(PeriodoAplicado, '-01'))",
     "sin_datos_rescatados": "_rescued_data IS NULL"})

con_cuarentena(
    "mora", "Foto de cartera por aportante al cierre de cada mes.",
    lambda: con_fk_aportante(tipar(leer_bronze("mora"), {
        "MoraID": "BIGINT", "PeriodosAdeudados": "INT", "DiasMora": "INT",
        "SaldoCapital": "DECIMAL(18,2)", "SaldoInteres": "DECIMAL(18,2)",
        "SaldoTotal": "DECIMAL(18,2)", "TieneAcuerdo": "BOOLEAN"})),
    {"aportante_existe": "_ap_ok IS NOT NULL",
     "dias_no_negativos": "DiasMora >= 0",
     "tramo_valido": f"EstadoMora IN ({TRAMOS})",
     "tramo_coherente": "(DiasMora = 0) = (EstadoMora = 'Sin Mora')",
     "sin_datos_rescatados": "_rescued_data IS NULL"})

con_cuarentena(
    "cobranza", "Gestiones de cobranza con su resultado y nota del asesor.",
    lambda: con_fk_aportante(tipar(leer_bronze("cobranza"), {
        "GestionID": "BIGINT", "FechaGestion": "DATE", "AcuerdoID": "BIGINT"})),
    {"aportante_existe": "_ap_ok IS NOT NULL",
     "tipo_valido": "TipoGestion IN ('Llamada','Correo','Visita','AcuerdoPago')",
     "resultado_valido": "ResultadoGestion IN ('Exitosa','Pendiente','Sin Contacto','Rechazada')",
     "fecha_valida": "FechaGestion IS NOT NULL",
     "sin_datos_rescatados": "_rescued_data IS NULL"})

con_cuarentena(
    "acuerdo_pago", "Cuotas de los acuerdos de pago con su estado al corte.",
    lambda: con_fk_aportante(tipar(leer_bronze("acuerdo_pago"), {
        "AcuerdoID": "BIGINT", "FechaAcuerdo": "DATE", "NumeroCuota": "INT",
        "FechaVencimientoCuota": "DATE", "ValorCuota": "DECIMAL(18,2)",
        "ValorPagado": "DECIMAL(18,2)", "FechaPagoCuota": "DATE"})),
    {"aportante_existe": "_ap_ok IS NOT NULL",
     "cuota_valida": "NumeroCuota BETWEEN 1 AND 12",
     "pagado_no_supera_cuota": "ValorPagado <= ValorCuota",
     "estado_valido": "EstadoCuota IN ('Pagada','Vencida','Pendiente','Anulada')",
     "sin_datos_rescatados": "_rescued_data IS NULL"})

con_cuarentena(
    "riesgo", "Score de probabilidad de incumplimiento por aportante y mes.",
    lambda: con_fk_aportante(tipar(leer_bronze("riesgo"), {
        "FechaEvaluacion": "DATE", "ProbabilidadIncumplimiento": "DOUBLE"})),
    {"aportante_existe": "_ap_ok IS NOT NULL",
     "probabilidad_valida": "ProbabilidadIncumplimiento BETWEEN 0 AND 1",
     "nivel_valido": "NivelRiesgo IN ('Bajo','Medio','Alto','Critico')",
     "sin_datos_rescatados": "_rescued_data IS NULL"})
