"""Preparación compartida del modelo de probabilidad de incumplimiento.

Lo usan los tres notebooks de ML, para que features, transformaciones y
métricas sean idénticas al entrenar y al calificar:
    04_ml_features.py      -> construir_features()
    05_ml_entrenamiento.py -> dividir(), matriz(), modelo_*(), metricas()
    06_ml_scoring.py       -> matriz(), contribuciones(), razones(), nivel()

Regla de oro: una feature del mes t solo puede usar información que el fondo
ya conocía al cierre de t. Por eso se construyen con:
  - fotos mensuales (fact_mora al corte de cada mes),
  - eventos con fecha (pagos, gestiones, cuotas) filtrados por fecha <= corte,
  - el valor esperado de cada periodo (se conoce al cerrar el periodo).
Nunca se usa EstadoPago, ValorPagado ni EstadoAcuerdo: reflejan el estado al
corte final de los datos, no el de cada mes, y filtrarían el futuro.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# Spark solo se necesita para construir_features (corre en Databricks). Sin Spark
# (pruebas en GitHub Actions, o el ambiente limpio donde se carga el modelo
# registrado, que lleva este archivo empaquetado) el resto del módulo funciona.
try:
    from pyspark.sql import Window
    from pyspark.sql import functions as F
except ImportError:
    Window = F = None

# -----------------------------------------------------------------------------
# Definiciones
# -----------------------------------------------------------------------------
# Etiqueta: 1 si el aportante tiene mora > 30 días en algún cierre de t+1, t+2 o t+3.
# Por qué 3 meses y no solo t+1: una empresa al día al cierre de t no puede acumular
# más de ~29 días de mora al cierre de t+1 (su siguiente vencimiento cae entre el 2.º
# y el 16.º día hábil). Con t+1 el modelo solo "confirmaría" a quien ya está en mora.
ETIQUETA = "Mora30Prox3m"
HORIZONTE = 3
LLAVES = ["AportanteID", "Periodo", "FechaCorte"]

# Partición temporal: nunca al azar, porque mezclaría el futuro con el pasado.
# Entre bloques se dejan 2-3 meses de separación para que la etiqueta (que mira 3
# meses adelante) de un bloque no se cruce con los meses del siguiente.
FIN_ENTRENAMIENTO = "2024-09"         # etiquetas hasta 2024-12
VALIDACION = ("2025-01", "2025-03")   # elegir modelo y parar el entrenamiento
PRUEBA = ("2025-06", "2025-09")       # reporte final, nunca se usa para decidir
PRODUCCION = "2025-10"                # desde aquí no hay 3 meses de futuro: solo se califica

UMBRALES = {"Medio": 0.15, "Alto": 0.40, "Critico": 0.75}   # los mismos de reglas-v1

CATEGORIAS = {
    "ActividadEconomica": ["Comercio", "Servicios", "Industria", "Construcción", "Transporte",
                           "Agro", "Salud", "Educación", "Tecnología"],
    "TamanoEmpresa": ["Micro", "Pequeña", "Mediana", "Grande"],
    "Regional": ["Bogotá", "Antioquia y Eje Cafetero", "Caribe", "Pacífico", "Centro Oriente"],
}

# Feature -> (etiqueta legible, formato). Formatos: n entero, d decimal, cop pesos,
# pct porcentaje, si sí/no, cat categoría.
DESCRIPCION = {
    "DiasMora": ("Días de mora al corte", "n"),
    "PeriodosAdeudados": ("Periodos adeudados", "n"),
    "SaldoTotal": ("Saldo vencido", "cop"),
    "SaldoEnMesesDeAporte": ("Deuda en meses de aporte", "d"),
    "EnMora": ("En mora al corte", "si"),
    "TieneAcuerdo": ("Acuerdo de pago vigente", "si"),
    "MesesEnMora3m": ("Meses en mora (últimos 3)", "n"),
    "MesesEnMora6m": ("Meses en mora (últimos 6)", "n"),
    "MesesEnMora12m": ("Meses en mora (últimos 12)", "n"),
    "MesesMora30_12m": ("Meses con mora > 30 días (últimos 12)", "n"),
    "MaxDiasMora12m": ("Máximo de días de mora (últimos 12)", "n"),
    "DiasMoraMesAnterior": ("Días de mora del mes anterior", "n"),
    "CambioSaldo": ("Cambio del saldo frente al mes anterior", "cop"),
    "EmpeoroTramo": ("Empeoró de tramo este mes", "si"),
    "MesesHistoria": ("Meses de historia disponibles", "n"),
    "Cotizantes": ("Cotizantes en el periodo", "n"),
    "ValorEsperado": ("Valor esperado del periodo", "cop"),
    "TendenciaCotizantes": ("Cotizantes frente a hace 3 meses", "pct"),
    "AntiguedadAnios": ("Antigüedad (años)", "d"),
    "TasaPagoOportuno12m": ("Periodos pagados completos a tiempo (últimos 12)", "pct"),
    "PeriodosEvaluados12m": ("Periodos evaluados (últimos 12)", "n"),
    "PagoATiempoUltimoPeriodo": ("Pagó completo y a tiempo el último periodo", "si"),
    "PagosExtemporaneos12m": ("Pagos extemporáneos (últimos 12 meses)", "n"),
    "InteresPagado12m": ("Intereses de mora pagados (últimos 12 meses)", "cop"),
    "AbonosAcuerdo3m": ("Abonos de acuerdo (últimos 3 meses)", "n"),
    "Gestiones3m": ("Gestiones de cobranza (últimos 3 meses)", "n"),
    "GestionesExitosas3m": ("Gestiones exitosas (últimos 3 meses)", "n"),
    "GestionesRechazadas3m": ("Gestiones rechazadas (últimos 3 meses)", "n"),
    "GestionesSinContacto3m": ("Gestiones sin contacto (últimos 3 meses)", "n"),
    "Visitas3m": ("Visitas (últimos 3 meses)", "n"),
    "AcuerdosFirmados12m": ("Acuerdos firmados (últimos 12 meses)", "n"),
    "CuotasVencidasImpagas": ("Cuotas de acuerdo vencidas sin pagar", "n"),
    "ActividadEconomica": ("Sector", "cat"),
    "TamanoEmpresa": ("Tamaño de empresa", "cat"),
    "Regional": ("Regional", "cat"),
}
CATEGORICAS = list(CATEGORIAS)
NUMERICAS = [f for f in DESCRIPCION if f not in CATEGORIAS]
FEATURES = NUMERICAS + CATEGORICAS
# Se transforman con log para que montos muy distintos sean comparables.
LOGARITMICAS = ["SaldoTotal", "ValorEsperado", "InteresPagado12m", "Cotizantes"]
# Pueden quedar vacías con sentido (sin historia suficiente): no se rellenan con 0.
CON_NULOS = ["SaldoEnMesesDeAporte", "DiasMoraMesAnterior", "TendenciaCotizantes",
             "TasaPagoOportuno12m", "PagoATiempoUltimoPeriodo"]


# -----------------------------------------------------------------------------
# 1. Construcción de features (Spark)
# -----------------------------------------------------------------------------
def _mes(col):
    """'2024-06' -> número de mes continuo (2024*12+6), para ventanas por rango."""
    return F.substring(col, 1, 4).cast("int") * 12 + F.substring(col, 6, 2).cast("int")


def _mes_fecha(col):
    return F.year(col) * 12 + F.month(col)


def _fecha_key(col):
    return F.to_date(F.col(col).cast("string"), "yyyyMMdd")


def construir_features(spark, catalogo: str):
    """Una fila por aportante y mes de corte, con features y etiqueta."""
    g = f"{catalogo}.gold"
    por_ap = Window.partitionBy("AportanteID").orderBy("Mes")

    def ultimos(n):  # los últimos n meses, incluido el actual
        return por_ap.rangeBetween(-(n - 1), 0)

    def previo(col):  # valor del mes anterior, solo si es exactamente t-1
        return F.when(F.lag("Mes").over(por_ap) == F.col("Mes") - 1, F.lag(col).over(por_ap))

    # --- Base: foto de cartera al cierre de cada mes -------------------------
    base = (spark.table(f"{g}.fact_mora")
            .select("AportanteID", "AportanteSK", "Periodo", "DiasMora", "PeriodosAdeudados",
                    "SaldoTotal", "OrdenTramo",
                    F.col("EnMora").cast("int").alias("EnMora"),
                    F.col("TieneAcuerdo").cast("int").alias("TieneAcuerdo"))
            .withColumn("Mes", _mes(F.col("Periodo")))
            .withColumn("FechaCorte", F.last_day(F.to_date(F.concat("Periodo", F.lit("-01"))))))

    base = (base
            .withColumn("MesesEnMora3m", F.sum("EnMora").over(ultimos(3)))
            .withColumn("MesesEnMora6m", F.sum("EnMora").over(ultimos(6)))
            .withColumn("MesesEnMora12m", F.sum("EnMora").over(ultimos(12)))
            .withColumn("MesesMora30_12m", F.sum((F.col("DiasMora") > 30).cast("int")).over(ultimos(12)))
            .withColumn("MaxDiasMora12m", F.max("DiasMora").over(ultimos(12)))
            .withColumn("MesesHistoria", F.count("*").over(ultimos(12)))
            .withColumn("DiasMoraMesAnterior", previo("DiasMora"))
            .withColumn("CambioSaldo", F.col("SaldoTotal") - F.coalesce(previo("SaldoTotal"), F.lit(0)))
            .withColumn("EmpeoroTramo",
                        (F.col("OrdenTramo") > F.coalesce(previo("OrdenTramo"), F.lit(0))).cast("int"))
            # Etiqueta: ¿mora > 30 días en algún cierre de los próximos 3 meses?
            # Si la empresa no aparece en un mes es porque no tiene deuda (cuenta como 0).
            .withColumn(ETIQUETA, F.greatest(*[
                F.coalesce(((F.lead("Mes", k).over(por_ap) <= F.col("Mes") + HORIZONTE)
                            & (F.lead("DiasMora", k).over(por_ap) > 30)).cast("int"), F.lit(0))
                for k in range(1, HORIZONTE + 1)])))
    # Sin 3 meses de futuro observado (los últimos meses de los datos) no hay etiqueta.
    ultimo_mes = base.agg(F.max("Mes")).first()[0]
    base = base.withColumn(ETIQUETA, F.when(F.col("Mes") + HORIZONTE <= ultimo_mes, F.col(ETIQUETA)))

    # --- Atributos de la empresa en la versión vigente a esa fecha (SCD2) ----
    atributos = spark.table(f"{g}.dim_aportante").select(
        "AportanteSK", "ActividadEconomica", "TamanoEmpresa", "Regional", "FechaAfiliacion")
    base = (base.join(atributos, "AportanteSK", "left")
            .withColumn("AntiguedadAnios",
                        F.round(F.months_between("FechaCorte", "FechaAfiliacion") / 12, 2))
            .drop("FechaAfiliacion"))

    # --- Tamaño de la obligación: cotizantes y valor esperado ---------------
    aportes = (spark.table(f"{g}.fact_aporte")
               .select("AportanteID", "Periodo", "ValorEsperado",
                       F.col("NumeroCotizantes").alias("Cotizantes"))
               .withColumn("Mes", _mes(F.col("Periodo"))).drop("Periodo"))
    aportes = aportes.withColumn("EsperadoProm3m", F.avg("ValorEsperado").over(ultimos(3)))
    hace3 = aportes.select("AportanteID", (F.col("Mes") + 3).alias("Mes"),
                           F.col("Cotizantes").alias("_cot_hace3"))
    base = (base.join(aportes, ["AportanteID", "Mes"], "left")
            .join(hace3, ["AportanteID", "Mes"], "left")
            .withColumn("TendenciaCotizantes", F.try_divide(F.col("Cotizantes"), F.col("_cot_hace3")))
            .withColumn("SaldoEnMesesDeAporte", F.try_divide(F.col("SaldoTotal"), F.col("EsperadoProm3m")))
            .drop("_cot_hace3", "EsperadoProm3m"))

    # --- Puntualidad: ¿cada periodo se pagó completo antes de su fecha límite?
    # El periodo p vence en el mes p+1, así que al cierre de t se conoce hasta p = t-1.
    pagos = (spark.table(f"{g}.fact_pago")
             .withColumn("FechaPago", _fecha_key("FechaPagoKey"))
             .withColumn("MesPago", _mes_fecha(F.col("FechaPago"))))
    oportuno = (pagos.where("TipoPago = 'PILA oportuna'")
                .groupBy("AportanteID", F.col("PeriodoAplicado").alias("Periodo"))
                .agg(F.sum("ValorCapital").alias("_pagado_oportuno")))
    punt = (spark.table(f"{g}.fact_aporte").select("AportanteID", "Periodo", "ValorEsperado")
            .join(oportuno, ["AportanteID", "Periodo"], "left")
            .withColumn("_a_tiempo", (F.coalesce("_pagado_oportuno", F.lit(0))
                                      >= F.col("ValorEsperado") - 1).cast("int"))
            .withColumn("Mes", _mes(F.col("Periodo"))))
    punt = (punt
            .withColumn("PeriodosEvaluados12m", F.count("*").over(ultimos(12)))
            .withColumn("_a_tiempo_12m", F.sum("_a_tiempo").over(ultimos(12)))
            .select("AportanteID", (F.col("Mes") + 1).alias("Mes"),   # se conoce en el mes siguiente
                    "PeriodosEvaluados12m",
                    F.try_divide(F.col("_a_tiempo_12m"), F.col("PeriodosEvaluados12m")).alias("TasaPagoOportuno12m"),
                    F.col("_a_tiempo").alias("PagoATiempoUltimoPeriodo")))
    base = base.join(punt, ["AportanteID", "Mes"], "left")

    # --- Eventos mensuales: pagos y gestiones (fecha <= corte) ---------------
    pagos_mes = (pagos.groupBy("AportanteID", F.col("MesPago").alias("Mes"))
                 .agg(F.count_if(F.col("TipoPago") == "PILA extemporánea").alias("_extemp"),
                      F.sum("ValorInteres").alias("_interes"),
                      F.count_if(F.col("TipoPago") == "Abono acuerdo").alias("_abonos")))
    gestiones = (spark.table(f"{g}.fact_cobranza")
                 .withColumn("Mes", _mes_fecha(_fecha_key("FechaGestionKey")))
                 .groupBy("AportanteID", "Mes")
                 .agg(F.count("*").alias("_gest"),
                      F.count_if(F.col("ResultadoGestion") == "Exitosa").alias("_exit"),
                      F.count_if(F.col("ResultadoGestion") == "Rechazada").alias("_rech"),
                      F.count_if(F.col("ResultadoGestion") == "Sin Contacto").alias("_sinc"),
                      F.count_if(F.col("TipoGestion") == "Visita").alias("_visit"),
                      F.count("AcuerdoID").alias("_acuerdos")))
    eventos = ["_extemp", "_interes", "_abonos", "_gest", "_exit", "_rech", "_sinc", "_visit", "_acuerdos"]
    base = (base.join(pagos_mes, ["AportanteID", "Mes"], "left")
            .join(gestiones, ["AportanteID", "Mes"], "left")
            .fillna(0, subset=eventos)
            .withColumn("PagosExtemporaneos12m", F.sum("_extemp").over(ultimos(12)))
            .withColumn("InteresPagado12m", F.sum("_interes").over(ultimos(12)))
            .withColumn("AbonosAcuerdo3m", F.sum("_abonos").over(ultimos(3)))
            .withColumn("Gestiones3m", F.sum("_gest").over(ultimos(3)))
            .withColumn("GestionesExitosas3m", F.sum("_exit").over(ultimos(3)))
            .withColumn("GestionesRechazadas3m", F.sum("_rech").over(ultimos(3)))
            .withColumn("GestionesSinContacto3m", F.sum("_sinc").over(ultimos(3)))
            .withColumn("Visitas3m", F.sum("_visit").over(ultimos(3)))
            .withColumn("AcuerdosFirmados12m", F.sum("_acuerdos").over(ultimos(12)))
            .drop(*eventos))

    # --- Cuotas de acuerdo vencidas y sin pagar al corte ---------------------
    cuotas = (spark.table(f"{g}.fact_acuerdo_cuota")
              .select("AportanteID",
                      _fecha_key("FechaAcuerdoKey").alias("_firma"),
                      _fecha_key("FechaVencimientoKey").alias("_venc"),
                      _fecha_key("FechaPagoCuotaKey").alias("_pago")))
    cortes = base.select("AportanteID", "Mes", "FechaCorte")
    impagas = (cortes.join(cuotas, "AportanteID")
               .where((F.col("_firma") <= F.col("FechaCorte")) & (F.col("_venc") <= F.col("FechaCorte"))
                      & (F.col("_pago").isNull() | (F.col("_pago") > F.col("FechaCorte"))))
               .groupBy("AportanteID", "Mes").agg(F.count("*").alias("CuotasVencidasImpagas")))
    base = (base.join(impagas, ["AportanteID", "Mes"], "left")
            .fillna(0, subset=["CuotasVencidasImpagas", "Cotizantes", "ValorEsperado"]))

    return base.select(*LLAVES, "AportanteSK", *FEATURES, ETIQUETA)


# -----------------------------------------------------------------------------
# 2. Matriz para el modelo (pandas)
# -----------------------------------------------------------------------------
def matriz(pdf: pd.DataFrame) -> pd.DataFrame:
    """Aplica las mismas transformaciones al entrenar y al calificar."""
    X = pdf[FEATURES].copy()
    for c in NUMERICAS:
        X[c] = pd.to_numeric(X[c], errors="coerce").astype("float64")
    rellenar = [c for c in NUMERICAS if c not in CON_NULOS]
    X[rellenar] = X[rellenar].fillna(0)
    for c in LOGARITMICAS:
        X[c] = np.log1p(X[c].clip(lower=0))
    X["CambioSaldo"] = np.sign(X["CambioSaldo"]) * np.log1p(X["CambioSaldo"].abs())
    for c, valores in CATEGORIAS.items():
        X[c] = pd.Categorical(X[c], categories=valores)
    return X


def dividir(pdf: pd.DataFrame):
    """Partición temporal en entrenamiento, validación y prueba (solo filas con etiqueta)."""
    con_etiqueta = pdf[pdf[ETIQUETA].notna()]
    p = con_etiqueta["Periodo"]
    entrenamiento = con_etiqueta[p <= FIN_ENTRENAMIENTO]
    validacion = con_etiqueta[(p >= VALIDACION[0]) & (p <= VALIDACION[1])]
    prueba = con_etiqueta[(p >= PRUEBA[0]) & (p <= PRUEBA[1])]
    return entrenamiento, validacion, prueba


def muestra(periodo: pd.Series) -> pd.Series:
    """Etiqueta de partición de cada fila, para saber qué scores son 'fuera de muestra'."""
    return np.select(
        [periodo <= FIN_ENTRENAMIENTO, periodo < VALIDACION[0], periodo <= VALIDACION[1],
         periodo < PRUEBA[0], periodo <= PRUEBA[1]],
        ["entrenamiento", "separacion", "validacion", "separacion", "prueba"], "produccion")


# -----------------------------------------------------------------------------
# 3. Modelos
# -----------------------------------------------------------------------------
def modelo_logistico():
    """Línea base interpretable: regresión logística con escalado y one-hot."""
    from sklearn.compose import ColumnTransformer
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    prep = ColumnTransformer([
        ("num", Pipeline([("imputar", SimpleImputer(strategy="median", add_indicator=True)),
                          ("escalar", StandardScaler())]), NUMERICAS),
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAS),
    ])
    return Pipeline([("prep", prep), ("lr", LogisticRegression(max_iter=2000, C=1.0))])


PARAMS_LGBM = dict(n_estimators=1500, learning_rate=0.03, num_leaves=31, min_child_samples=200,
                   subsample=0.8, subsample_freq=1, colsample_bytree=0.8, reg_lambda=1.0,
                   random_state=42, verbose=-1)


def modelo_lightgbm(**cambios):
    """Árboles con boosting: capta interacciones (p. ej. sector x días de mora)."""
    from lightgbm import LGBMClassifier
    return LGBMClassifier(**{**PARAMS_LGBM, **cambios})


def empaquetar(estimador_entrenado, datos_entrenamiento: pd.DataFrame):
    """Une la preparación (matriz) y el estimador ya entrenado en un solo modelo.

    El modelo que se registra en MLflow recibe las features originales (como
    están en la tabla) y hace él mismo las transformaciones. Así el scoring no
    puede aplicar una transformación distinta a la del entrenamiento.
    """
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import FunctionTransformer
    paso = FunctionTransformer(matriz, validate=False).fit(datos_entrenamiento[FEATURES])
    return Pipeline([("matriz", paso), ("modelo", estimador_entrenado)])


def entrenar_lightgbm(X_tr, y_tr, X_val, y_val, **cambios):
    """Entrena con parada temprana en validación: se detiene cuando deja de mejorar."""
    import lightgbm as lgb
    modelo = modelo_lightgbm(**cambios)
    modelo.fit(X_tr, y_tr, eval_set=[(X_val, y_val)], eval_metric="average_precision",
               callbacks=[lgb.early_stopping(100, verbose=False)])
    return modelo


# -----------------------------------------------------------------------------
# 4. Métricas
# -----------------------------------------------------------------------------
def metricas(y, p) -> dict:
    """Métricas de un score de riesgo.

    auc_roc   qué tan bien ordena (0,5 = azar, 1 = perfecto)
    auc_pr    precisión promedio: más exigente cuando los positivos son minoría
    ks        máxima separación entre buenos y malos (estándar en riesgo de crédito)
    recall_top10  % de los incumplidos que caen en el 10% con score más alto
    lift_top10    cuántas veces la tasa de incumplimiento del top 10% supera la media
    brier     error cuadrático de la probabilidad (calibración; menor es mejor)
    """
    from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score, roc_curve
    y = np.asarray(y).astype(int)
    p = np.asarray(p, dtype=float)
    fpr, tpr, _ = roc_curve(y, p)
    corte = np.quantile(p, 0.9)
    top = p >= corte
    return {
        "auc_roc": float(roc_auc_score(y, p)),
        "auc_pr": float(average_precision_score(y, p)),
        "ks": float(np.max(tpr - fpr)),
        "recall_top10": float(y[top].sum() / max(y.sum(), 1)),
        "lift_top10": float(y[top].mean() / max(y.mean(), 1e-9)),
        "brier": float(brier_score_loss(y, np.clip(p, 0, 1))),
        "tasa_real": float(y.mean()),
        "prob_media": float(p.mean()),
        "filas": int(len(y)),
    }


def metricas_por_segmento(df: pd.DataFrame, columna_score: str) -> dict:
    """Métricas en toda la población y por situación actual del aportante.

    al_dia_hoy   prevención: ¿quién va a caer en mora? (el reto más difícil)
    en_mora_hoy  cobranza: ¿quién se va a hundir y quién se va a recuperar?
    """
    salida = {}
    for nombre, filtro in (("todos", df["DiasMora"] >= 0), ("al_dia_hoy", df["DiasMora"] == 0),
                           ("en_mora_hoy", df["DiasMora"] > 0)):
        m = metricas(df.loc[filtro, ETIQUETA], df.loc[filtro, columna_score])
        salida.update({f"{nombre}_{k}": v for k, v in m.items()})
    return salida


def tabla_deciles(y, p, n=10) -> pd.DataFrame:
    """Tasa de incumplimiento real por decil de score (1 = riesgo más alto)."""
    d = pd.DataFrame({"y": np.asarray(y).astype(int), "p": np.asarray(p, dtype=float)})
    d["decil"] = n - pd.qcut(d["p"].rank(method="first"), n, labels=False)
    t = d.groupby("decil").agg(filas=("y", "size"), incumplen=("y", "sum"),
                               tasa_real=("y", "mean"), prob_media=("p", "mean"))
    t["lift"] = t["tasa_real"] / d["y"].mean()
    t["captura_acumulada"] = t["incumplen"].cumsum() / d["y"].sum()
    return t.reset_index()


# -----------------------------------------------------------------------------
# 5. Explicaciones
# -----------------------------------------------------------------------------
def contribuciones(modelo, datos: pd.DataFrame) -> pd.DataFrame:
    """Aporte de cada feature al score de cada fila, en log-odds.

    Recibe el modelo empaquetado (ver empaquetar) y las features originales.
    LightGBM las calcula de forma exacta (valores SHAP con pred_contrib=True).
    En la regresión logística, el aporte es coeficiente x valor escalado,
    sumando las columnas one-hot de cada categoría.
    """
    X = modelo.named_steps["matriz"].transform(datos[FEATURES])
    estimador = modelo.named_steps["modelo"]
    if hasattr(estimador, "booster_"):
        c = estimador.predict(X, pred_contrib=True)
        return pd.DataFrame(c[:, :len(FEATURES)], columns=FEATURES, index=X.index)
    prep, lr = estimador.named_steps["prep"], estimador.named_steps["lr"]
    Xt = prep.transform(X)
    aporte = Xt * lr.coef_.ravel()
    nombres = prep.get_feature_names_out()
    salida = pd.DataFrame(0.0, index=X.index, columns=FEATURES)
    for j, nombre in enumerate(nombres):
        bloque, col = nombre.split("__", 1)
        if bloque == "cat":
            col = next(c for c in CATEGORICAS if col.startswith(c + "_"))
        elif col.startswith("missingindicator_"):
            col = col.replace("missingindicator_", "")
        salida[col] += aporte[:, j]
    return salida


def _formato(valor, tipo):
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return "sin dato"
    if tipo == "cat":
        return str(valor)
    if tipo == "si":
        return "sí" if float(valor) >= 0.5 else "no"
    if tipo == "pct":
        return f"{float(valor) * 100:.0f}%"
    if tipo == "cop":
        return "$" + f"{float(valor):,.0f}".replace(",", ".")
    if tipo == "d":
        return f"{float(valor):.1f}".replace(".", ",")
    return f"{float(valor):.0f}"


def razones(contrib: pd.DataFrame, pdf: pd.DataFrame, n: int = 3) -> list[list[str]]:
    """Las n features que más empujan el riesgo hacia arriba, en texto legible.

    Usa los valores originales (antes del log) para que el texto sea entendible:
    'Días de mora al corte: 47', 'Sector: Construcción'.
    """
    valores = contrib.to_numpy()
    orden = np.argsort(-valores, axis=1)[:, :n]
    originales = pdf[FEATURES].to_numpy(dtype=object)
    salida = []
    for i in range(len(contrib)):
        textos = []
        for j in orden[i]:
            if valores[i, j] <= 0:
                break
            etiqueta, tipo = DESCRIPCION[FEATURES[j]]
            textos.append(f"{etiqueta}: {_formato(originales[i, j], tipo)}")
        salida.append(textos)
    return salida


def nivel(p) -> np.ndarray:
    p = np.asarray(p, dtype=float)
    return np.select([p < UMBRALES["Medio"], p < UMBRALES["Alto"], p < UMBRALES["Critico"]],
                     ["Bajo", "Medio", "Alto"], "Critico")
