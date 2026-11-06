"""Gold: modelo estrella y agregados de negocio — Lakeflow Spark Declarative Pipelines.

Lee las tablas limpias de Silver y publica en el esquema gold:

  Dimensiones   dim_aportante (SCD2), dim_cotizante, dim_tiempo, dim_geografia
  Hechos        fact_aporte_detalle, fact_aporte, fact_pago, fact_mora,
                fact_cobranza, fact_acuerdo_cuota, fact_riesgo
  Agregados     agg_recaudo_mensual, agg_cartera_mensual, agg_transicion_tramos,
                agg_efectividad_cobranza, agg_indicadores_mensuales, agg_desempeno_score

Decisiones de diseño:
  - Llaves sustitutas (…SK) con xxhash64: deterministas, así una nueva
    ejecución produce las mismas llaves y no rompe los informes.
  - Cada hecho se une a la VERSIÓN del aportante vigente en la fecha del hecho
    (SCD2). La mora de enero de 2024 queda en la ciudad que la empresa tenía
    en enero de 2024, no en la de hoy.
  - Todo es materialized view: Gold se recalcula a partir de Silver, y
    Databricks lo hace de forma incremental cuando puede.
  - Gold no expone datos personales: el número de documento se publica como hash.

Configuración del pipeline: catalogo = recaudo_dev; esquema por defecto: gold.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

CATALOGO = spark.conf.get("catalogo", "recaudo_dev")
SILVER = f"{CATALOGO}.silver"
FIN_CALENDARIO = "2026-12-31"     # cubre los vencimientos de cuotas firmadas a fines de 2025


# Todas las razones usan F.try_divide: si el denominador es 0 el resultado es NULL
# en lugar de un error (Spark con modo ANSI falla al dividir por cero).


def silver(tabla):
    return spark.read.table(f"{SILVER}.{tabla}")


def tabla_existe(esquema, tabla):
    """¿Existe la tabla en el catálogo?

    Dentro de una pipeline no se puede usar spark.catalog.tableExists: Databricks
    bloquea esa API (error PY4J_BLOCKED_API). Se pregunta al information_schema,
    que es una consulta SQL normal.
    """
    return len(spark.sql(f"""
        SELECT 1 FROM {CATALOGO}.information_schema.tables
        WHERE table_schema = '{esquema}' AND table_name = '{tabla}'
    """).take(1)) > 0


# Se evalúa una sola vez, al armar el grafo de la pipeline.
HAY_SCORES_ML = tabla_existe("ml", "score_riesgo")


def fecha_key(col):
    """Fecha -> entero yyyymmdd, la llave de dim_tiempo."""
    return F.date_format(col, "yyyyMMdd").cast("int")


def fin_de_periodo(col_periodo):
    """'2024-06' -> 2024-06-30: fecha de corte de un periodo."""
    return F.last_day(F.to_date(F.concat(col_periodo, F.lit("-01"))))


def con_version_aportante(df, fecha):
    """Agrega AportanteSK: la versión del aportante vigente en 'fecha'."""
    versiones = (spark.read.table("dim_aportante")
                 .select("AportanteID", "AportanteSK", "ValidoDesde", "ValidoHasta"))
    cond = ((df["AportanteID"] == versiones["AportanteID"])
            & (fecha >= versiones["ValidoDesde"])
            & (fecha < F.coalesce(versiones["ValidoHasta"], F.lit("9999-12-31").cast("date"))))
    return (df.join(versiones, cond, "inner")
              .drop(versiones["AportanteID"]).drop("ValidoDesde", "ValidoHasta"))


# =============================================================================
# Dimensiones
# =============================================================================
@dp.materialized_view(name="dim_geografia", comment="Municipios con departamento y regional.")
def dim_geografia():
    return silver("geografia").select("GeografiaID", "Regional", "Departamento", "CodigoDANE", "Ciudad")


@dp.materialized_view(name="dim_tiempo",
                      comment="Calendario diario 2023-2026. Después de enero de 2026 los festivos no están cargados (EsFestivo nulo).")
def dim_tiempo():
    # Se genera el calendario completo y se le pegan los atributos de Silver;
    # las fechas que Silver no trae (2026 en adelante) se calculan aquí.
    fechas = spark.sql(f"SELECT explode(sequence(DATE'2023-01-01', DATE'{FIN_CALENDARIO}')) AS Fecha")
    base = silver("tiempo").select("Fecha", "NombreMes", "NombreDia", "EsFestivo", "EsDiaHabil")
    meses = F.element_at(F.array(*[F.lit(m) for m in [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto",
        "Septiembre", "Octubre", "Noviembre", "Diciembre"]]), F.month("Fecha"))
    dia_semana = (F.dayofweek("Fecha") + 5) % 7 + 1          # 1 = lunes
    dias = F.element_at(F.array(*[F.lit(d) for d in [
        "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]]), dia_semana)
    return (fechas.join(base, "Fecha", "left").select(
        fecha_key(F.col("Fecha")).alias("FechaKey"), "Fecha",
        F.year("Fecha").alias("Anio"), F.month("Fecha").alias("Mes"),
        F.coalesce("NombreMes", meses).alias("NombreMes"),
        F.quarter("Fecha").alias("Trimestre"), F.weekofyear("Fecha").alias("Semana"),
        F.dayofmonth("Fecha").alias("Dia"), dia_semana.alias("DiaSemana"),
        F.coalesce("NombreDia", dias).alias("NombreDia"),
        "EsFestivo",
        F.coalesce("EsDiaHabil", dia_semana <= 5).alias("EsDiaHabil"),
        F.date_format("Fecha", "yyyy-MM").alias("Periodo"),
        F.last_day("Fecha").alias("FechaCorteMes"),
        (F.col("Fecha") == F.last_day("Fecha")).alias("EsFinDeMes")))


@dp.materialized_view(name="dim_aportante",
                      comment="Aportantes con historial (SCD2): una fila por versión, con su ubicación y vigencia.")
def dim_aportante():
    ap = silver("aportante")
    geo = silver("geografia").select("GeografiaID", "Regional", "Departamento", "Ciudad")
    w = Window.partitionBy("AportanteID").orderBy("__START_AT")
    return (ap.join(geo, "GeografiaID", "left")
            .withColumn("_orden", F.row_number().over(w))
            .select(
                F.xxhash64("AportanteID", F.col("__START_AT").cast("string")).alias("AportanteSK"),
                "AportanteID", "Nit", "DigitoVerificacion", "DvValido", "RazonSocial",
                "ActividadEconomica", "CodigoCIIU",
                F.col("ActividadEconomica").isin("Construcción", "Transporte").alias("SectorRiesgoAlto"),
                "TamanoEmpresa", "FechaAfiliacion", "EstadoAportante",
                "GeografiaID", "Regional", "Departamento", "Ciudad",
                # La primera versión rige "desde siempre": así ningún hecho anterior
                # a la primera fecha registrada se queda sin aportante.
                F.when(F.col("_orden") == 1, F.lit("1900-01-01").cast("date"))
                 .otherwise(F.col("__START_AT")).alias("ValidoDesde"),
                F.col("__END_AT").alias("ValidoHasta"),
                F.col("__END_AT").isNull().alias("EsActual")))


@dp.materialized_view(name="dim_cotizante",
                      comment="Trabajadores afiliados. El documento se publica como hash: Gold no expone datos personales.")
def dim_cotizante():
    return (silver("cotizante").select(
        F.xxhash64("CotizanteID").alias("CotizanteSK"),
        "CotizanteID", F.col("AportanteID").alias("AportanteIDActual"), "TipoDocumento",
        F.sha2(F.concat_ws("|", "TipoDocumento", "NumeroDocumento"), 256).alias("DocumentoHash"),
        "Genero", "Edad",
        F.when(F.col("Edad") < 25, "18-24").when(F.col("Edad") < 35, "25-34")
         .when(F.col("Edad") < 45, "35-44").when(F.col("Edad") < 55, "45-54")
         .otherwise("55+").alias("RangoEdad"),
        "Salario", "TipoCotizante", "FechaIngreso", "FechaRetiro",
        F.col("FechaRetiro").isNull().alias("EsActivo")))


# =============================================================================
# Hechos
# =============================================================================
@dp.materialized_view(name="fact_aporte_detalle", cluster_by=["Periodo"],
                      comment="Aporte esperado por cotizante y periodo.")
def fact_aporte_detalle():
    df = silver("aporte_detalle").withColumn("FechaCorte", fin_de_periodo(F.col("Periodo")))
    df = con_version_aportante(df, F.col("FechaCorte"))
    return df.select(
        "Periodo", fecha_key(F.col("FechaCorte")).alias("FechaCorteKey"),
        "AportanteSK", "AportanteID", F.xxhash64("CotizanteID").alias("CotizanteSK"), "CotizanteID",
        "DiasCotizados", "IBC", "TarifaPension", "ValorCotizacion", "ValorFSP", "ValorEsperado")


@dp.materialized_view(name="fact_aporte", cluster_by=["Periodo"],
                      comment="Aporte esperado contra pagado por aportante y periodo, con su estado al corte.")
def fact_aporte():
    df = silver("aporte").withColumn("FechaCorte", fin_de_periodo(F.col("Periodo")))
    df = con_version_aportante(df, F.col("FechaCorte"))
    return df.select(
        "AporteID", "Periodo", fecha_key(F.col("FechaCorte")).alias("FechaCorteKey"),
        "AportanteSK", "AportanteID", "NumeroCotizantes", "ValorEsperado", "ValorPagado",
        (F.col("ValorEsperado") - F.col("ValorPagado")).alias("SaldoPendiente"),
        fecha_key(F.col("FechaLimitePago")).alias("FechaLimitePagoKey"),
        fecha_key(F.col("FechaPago")).alias("FechaPagoKey"),
        "DiasRetraso", "EstadoPago",
        (F.col("EstadoPago") == "Oportuno").alias("EsOportuno"),
        (F.col("EstadoPago") != "Pendiente").alias("EsVencido"))


@dp.materialized_view(name="fact_pago", cluster_by=["FechaPagoKey"],
                      comment="Transacciones de pago. EsCarteraHeredada = pagos de periodos de 2022.")
def fact_pago():
    df = con_version_aportante(silver("pago"), F.col("FechaPago"))
    return df.select(
        "PagoID", "AportanteSK", "AportanteID", "PeriodoAplicado",
        fecha_key(F.col("FechaPago")).alias("FechaPagoKey"),
        "ValorCapital", "ValorInteres", "ValorPagado", "TipoPago", "Canal", "AcuerdoID",
        "EsCarteraHeredada")


@dp.materialized_view(name="fact_mora", cluster_by=["Periodo"],
                      comment="Foto de cartera por aportante al cierre de cada mes.")
def fact_mora():
    df = silver("mora").withColumn("FechaCorte", fin_de_periodo(F.col("Periodo")))
    df = con_version_aportante(df, F.col("FechaCorte"))
    orden = F.when(F.col("EstadoMora") == "Sin Mora", 0).when(F.col("EstadoMora") == "1-30", 1) \
             .when(F.col("EstadoMora") == "31-60", 2).when(F.col("EstadoMora") == "61-90", 3).otherwise(4)
    return df.select(
        "MoraID", "Periodo", fecha_key(F.col("FechaCorte")).alias("FechaCorteKey"),
        "AportanteSK", "AportanteID", "PeriodosAdeudados", "DiasMora",
        "SaldoCapital", "SaldoInteres", "SaldoTotal", "EstadoMora", orden.alias("OrdenTramo"),
        (F.col("DiasMora") > 0).alias("EnMora"), "TieneAcuerdo")


@dp.materialized_view(name="fact_cobranza", cluster_by=["FechaGestionKey"],
                      comment="Gestiones de cobranza con resultado y nota del asesor.")
def fact_cobranza():
    df = con_version_aportante(silver("cobranza"), F.col("FechaGestion"))
    return df.select(
        "GestionID", "AportanteSK", "AportanteID",
        fecha_key(F.col("FechaGestion")).alias("FechaGestionKey"),
        F.date_format("FechaGestion", "yyyy-MM").alias("Periodo"),
        "TipoGestion", "ResultadoGestion", "TramoMoraAlGestionar", "Gestor", "NotaGestion", "AcuerdoID",
        (F.col("ResultadoGestion") == "Exitosa").alias("EsExitosa"),
        (F.col("ResultadoGestion") != "Sin Contacto").alias("HuboContacto"))


@dp.materialized_view(name="fact_acuerdo_cuota",
                      comment="Cuotas de acuerdos de pago con su estado al corte.")
def fact_acuerdo_cuota():
    df = con_version_aportante(silver("acuerdo_pago"), F.col("FechaAcuerdo"))
    return df.select(
        "AcuerdoID", "NumeroCuota", "AportanteSK", "AportanteID",
        fecha_key(F.col("FechaAcuerdo")).alias("FechaAcuerdoKey"),
        fecha_key(F.col("FechaVencimientoCuota")).alias("FechaVencimientoKey"),
        fecha_key(F.col("FechaPagoCuota")).alias("FechaPagoCuotaKey"),
        "ValorCuota", "ValorPagado", "EstadoCuota", "EstadoAcuerdo")


@dp.materialized_view(name="fact_riesgo", cluster_by=["FechaEvaluacionKey"],
                      comment=("Scores de probabilidad de incumplimiento por aportante y mes. ScoreVersion: "
                               "reglas-v1 (reglas del generador, horizonte t+1) y ml-v1 (modelo de ML, mora > 30 "
                               "días en los próximos 3 meses). Filtra siempre por ScoreVersion."))
def fact_riesgo():
    columnas = ["AportanteID", "FechaEvaluacion", "ProbabilidadIncumplimiento", "NivelRiesgo", "ScoreVersion"]
    df = silver("riesgo").select(*columnas)
    # Scores del modelo de ML (notebook 06). Si aún no existen, Gold solo trae reglas-v1.
    if HAY_SCORES_ML:
        df = df.unionByName(spark.read.table(f"{CATALOGO}.ml.score_riesgo").select(*columnas, "Muestra", "Razones"),
                            allowMissingColumns=True)
    for col, tipo in (("Muestra", "string"), ("Razones", "array<string>")):
        if col not in df.columns:   # reglas-v1 no tiene muestra ni razones
            df = df.withColumn(col, F.lit(None).cast(tipo))
    df = con_version_aportante(df, F.col("FechaEvaluacion"))
    return df.select(
        "AportanteSK", "AportanteID", fecha_key(F.col("FechaEvaluacion")).alias("FechaEvaluacionKey"),
        F.date_format("FechaEvaluacion", "yyyy-MM").alias("Periodo"),
        "ProbabilidadIncumplimiento", "NivelRiesgo", "ScoreVersion",
        F.col("Muestra").cast("string").alias("Muestra"),
        F.col("Razones").cast("array<string>").alias("Razones"))


# =============================================================================
# Agregados para tableros
# =============================================================================
def _atributos():
    return spark.read.table("dim_aportante").select(
        "AportanteSK", "Regional", "ActividadEconomica", "TamanoEmpresa")


@dp.materialized_view(name="agg_recaudo_mensual",
                      comment="Recaudo esperado vs. recaudado por periodo, regional, sector y tamaño.")
def agg_recaudo_mensual():
    return (spark.read.table("fact_aporte").join(_atributos(), "AportanteSK")
            .groupBy("Periodo", "Regional", "ActividadEconomica", "TamanoEmpresa")
            .agg(F.sum("ValorEsperado").alias("ValorEsperado"),
                 F.sum("ValorPagado").alias("ValorRecaudado"),
                 F.count("*").alias("Aportes"),
                 F.sum(F.col("EsOportuno").cast("int")).alias("AportesOportunos"),
                 F.sum(F.col("EsVencido").cast("int")).alias("AportesVencidos"),
                 F.sum("NumeroCotizantes").alias("Cotizantes"))
            .withColumn("PctCumplimiento", F.try_divide(F.col("ValorRecaudado"), F.col("ValorEsperado")))
            .withColumn("PctOportuno", F.try_divide(F.col("AportesOportunos"), F.col("AportesVencidos"))))


@dp.materialized_view(name="agg_cartera_mensual",
                      comment="Cartera por periodo de corte, regional, sector, tamaño y tramo de mora.")
def agg_cartera_mensual():
    return (spark.read.table("fact_mora").join(_atributos(), "AportanteSK")
            .groupBy("Periodo", "Regional", "ActividadEconomica", "TamanoEmpresa", "EstadoMora", "OrdenTramo")
            .agg(F.count("*").alias("Aportantes"),
                 F.sum("SaldoCapital").alias("SaldoCapital"),
                 F.sum("SaldoInteres").alias("SaldoInteres"),
                 F.sum("SaldoTotal").alias("SaldoTotal"),
                 F.sum(F.col("TieneAcuerdo").cast("int")).alias("AportantesConAcuerdo")))


@dp.materialized_view(name="agg_transicion_tramos",
                      comment="Matriz de transición (roll rate): de qué tramo a qué tramo pasa cada aportante de un mes al siguiente.")
def agg_transicion_tramos():
    w = Window.partitionBy("AportanteID").orderBy("Periodo")
    m = (spark.read.table("fact_mora")
         .select("AportanteID", "Periodo", "EstadoMora", "OrdenTramo", "SaldoTotal")
         .withColumn("PeriodoDestino", F.lead("Periodo").over(w))
         .withColumn("TramoDestino", F.lead("EstadoMora").over(w))
         .withColumn("OrdenTramoDestino", F.lead("OrdenTramo").over(w))
         .where("TramoDestino IS NOT NULL"))
    return (m.groupBy(F.col("Periodo").alias("PeriodoOrigen"), "PeriodoDestino",
                      F.col("EstadoMora").alias("TramoOrigen"), F.col("OrdenTramo").alias("OrdenTramoOrigen"),
                      "TramoDestino", "OrdenTramoDestino")
            .agg(F.count("*").alias("Aportantes"), F.sum("SaldoTotal").alias("SaldoOrigen"))
            .withColumn("Movimiento",
                        F.when(F.col("OrdenTramoDestino") > F.col("OrdenTramoOrigen"), "Empeora")
                         .when(F.col("OrdenTramoDestino") < F.col("OrdenTramoOrigen"), "Mejora")
                         .otherwise("Se mantiene")))


@dp.materialized_view(name="agg_efectividad_cobranza",
                      comment="Efectividad de la cobranza por mes, tipo de gestión y tramo de mora.")
def agg_efectividad_cobranza():
    return (spark.read.table("fact_cobranza")
            .groupBy("Periodo", "TipoGestion", "TramoMoraAlGestionar")
            .agg(F.count("*").alias("Gestiones"),
                 F.sum(F.col("EsExitosa").cast("int")).alias("Exitosas"),
                 F.sum(F.col("HuboContacto").cast("int")).alias("ConContacto"),
                 F.count("AcuerdoID").alias("AcuerdosFirmados"),
                 F.countDistinct("AportanteID").alias("AportantesGestionados"))
            .withColumn("TasaExito", F.try_divide(F.col("Exitosas"), F.col("Gestiones")))
            .withColumn("TasaContacto", F.try_divide(F.col("ConContacto"), F.col("Gestiones"))))


@dp.materialized_view(name="agg_indicadores_mensuales",
                      comment="KPIs ejecutivos por mes: recaudo, cartera vencida, índice de cartera y tasa de recuperación.")
def agg_indicadores_mensuales():
    aportes = (spark.read.table("fact_aporte").groupBy("Periodo")
               .agg(F.sum("ValorEsperado").alias("ValorEsperado"),
                    F.sum("ValorPagado").alias("ValorRecaudado"),
                    F.avg(F.when(F.col("EsVencido"), F.col("EsOportuno").cast("double"))).alias("PctOportuno")))
    mora = (spark.read.table("fact_mora").groupBy("Periodo")
            .agg(F.sum("SaldoCapital").alias("SaldoCapitalVencido"),
                 F.sum("SaldoTotal").alias("SaldoTotalVencido"),
                 F.avg(F.col("EnMora").cast("double")).alias("PctAportantesEnMora"),
                 F.sum(F.when(F.col("EstadoMora") == ">90", F.col("SaldoTotal")).otherwise(0)).alias("SaldoMas90")))
    # Recuperación: capital pagado en el mes sobre periodos ya vencidos (pagos
    # extemporáneos y abonos de acuerdos).
    recuperado = (spark.read.table("fact_pago")
                  .where("TipoPago <> 'PILA oportuna'")
                  .withColumn("Periodo", F.date_format(
                      F.to_date(F.col("FechaPagoKey").cast("string"), "yyyyMMdd"), "yyyy-MM"))
                  .groupBy("Periodo").agg(F.sum("ValorCapital").alias("CapitalRecuperado")))
    w = Window.orderBy("Periodo")
    w12 = Window.orderBy("Periodo").rowsBetween(-11, 0)
    return (aportes.join(mora, "Periodo", "left").join(recuperado, "Periodo", "left")
            .withColumn("PctCumplimiento", F.try_divide(F.col("ValorRecaudado"), F.col("ValorEsperado")))
            # Facturación anual: promedio mensual de los últimos 12 meses x 12. Con el
            # promedio, los primeros meses (con menos de 12 de historia) no se inflan.
            .withColumn("Esperado12m", F.avg("ValorEsperado").over(w12) * 12)
            # Índice de cartera vencida: capital vencido sobre la facturación anual.
            .withColumn("IndiceCarteraVencida", F.try_divide(F.col("SaldoCapitalVencido"), F.col("Esperado12m")))
            .withColumn("SaldoCapitalMesAnterior", F.lag("SaldoCapitalVencido").over(w))
            # Tasa de recuperación: lo recuperado en el mes sobre la cartera con la que abrió el mes.
            .withColumn("TasaRecuperacion", F.try_divide(F.col("CapitalRecuperado"), F.col("SaldoCapitalMesAnterior")))
            .withColumn("PctSaldoMas90", F.try_divide(F.col("SaldoMas90"), F.col("SaldoTotalVencido")))
            .drop("Esperado12m"))


@dp.materialized_view(name="agg_desempeno_score",
                      comment=("Desempeño de cada versión de score: tasa real de mora > 30 días en los 3 meses "
                               "siguientes, por periodo y decil de score (1 = más riesgoso)."))
def agg_desempeno_score():
    # Resultado observado: ¿tuvo mora > 30 días en algún cierre de los 3 meses siguientes?
    # Si el aportante no aparece en un mes es porque no tiene deuda (cuenta como 0).
    mes = F.substring("Periodo", 1, 4).cast("int") * 12 + F.substring("Periodo", 6, 2).cast("int")
    por_ap = Window.partitionBy("AportanteID").orderBy("Mes")
    mora = (spark.read.table("fact_mora").select("AportanteID", "Periodo", "DiasMora")
            .withColumn("Mes", mes)
            .withColumn("UltimoMes", F.max("Mes").over(Window.partitionBy(F.lit(1)))))
    futuro = [F.coalesce(((F.lead("Mes", k).over(por_ap) <= F.col("Mes") + 3)
                          & (F.lead("DiasMora", k).over(por_ap) > 30)).cast("int"), F.lit(0))
              for k in (1, 2, 3)]
    observado = (mora.withColumn("IncumpleProx3m", F.greatest(*futuro))
                 .where(F.col("Mes") + 3 <= F.col("UltimoMes"))
                 .select("AportanteID", "Periodo", "IncumpleProx3m"))
    decil = Window.partitionBy("ScoreVersion", "Periodo").orderBy(F.col("ProbabilidadIncumplimiento").desc())
    return (spark.read.table("fact_riesgo")
            .select("AportanteID", "Periodo", "ScoreVersion", "Muestra", "ProbabilidadIncumplimiento")
            .join(observado, ["AportanteID", "Periodo"])
            .withColumn("Decil", F.ntile(10).over(decil))
            .groupBy("ScoreVersion", "Periodo", "Muestra", "Decil")
            .agg(F.count("*").alias("Aportantes"),
                 F.sum("IncumpleProx3m").alias("Incumplen"),
                 F.avg("ProbabilidadIncumplimiento").alias("ProbabilidadMedia"))
            .withColumn("TasaReal", F.try_divide(F.col("Incumplen"), F.col("Aportantes"))))
