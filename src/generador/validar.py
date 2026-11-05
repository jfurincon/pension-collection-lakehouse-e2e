"""Valida que el dataset generado cumpla las 12 metas de correlación del plan.

Lee los CSV (sueltos o particionados por mes), aplica una limpieza mínima
(quita duplicados y montos negativos inyectados) y compara cada métrica con su
meta. Devuelve código de salida 1 si alguna falla, para usarlo en CI.

    python src/generador/validar.py --datos data/raw
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

TRAMOS = ["Sin Mora", "1-30", "31-60", "61-90", ">90"]


def leer(datos: Path, nombre: str, columnas=None) -> pd.DataFrame:
    archivo = datos / f"{nombre}.csv"
    if archivo.exists():
        return pd.read_csv(archivo, usecols=columnas, low_memory=False)
    partes = sorted((datos / nombre).glob("*.csv"))
    return pd.concat((pd.read_csv(p, usecols=columnas, low_memory=False) for p in partes), ignore_index=True)


def auc(score: np.ndarray, y: np.ndarray) -> float:
    """AUC por rangos (Mann-Whitney), sin dependencias extra."""
    rank = pd.Series(score).rank().to_numpy()
    pos = y.astype(bool)
    n1, n0 = pos.sum(), (~pos).sum()
    return float((rank[pos].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


def validar(datos: str | Path = "data/raw", config: str | Path = "config/parametros.yaml") -> bool:
    datos = Path(datos)
    cfg = yaml.safe_load(Path(config).read_text(encoding="utf-8"))
    smmlv = {int(k): v for k, v in cfg["pension"]["smmlv"].items()}

    dim_ap = leer(datos, "DimAportante").drop_duplicates("AportanteID", keep="last")
    dim_cot = leer(datos, "DimCotizante")
    aportes = leer(datos, "FactAportes")
    aportes = aportes[aportes["ValorPagado"] >= 0]
    mora = leer(datos, "FactMora")
    cob = leer(datos, "FactCobranza", ["AportanteID", "FechaGestion"])
    riesgo = leer(datos, "FactRiesgo")

    info = dim_ap.set_index("AportanteID")[["ActividadEconomica", "TamanoEmpresa", "FechaAfiliacion"]]
    mora = mora.join(info, on="AportanteID")
    corte_mes = pd.to_datetime(mora["Periodo"] + "-01") + pd.offsets.MonthEnd(0)
    mora["anios"] = (corte_mes - pd.to_datetime(mora["FechaAfiliacion"])).dt.days / 365.25
    mora["mora30"] = mora["DiasMora"] > 30
    tasa = mora["mora30"].mean()

    resultados = []

    def meta(n, nombre, valor, ok, objetivo, fmt="{:.2f}"):
        resultados.append((n, nombre, fmt.format(valor) if not isinstance(valor, str) else valor,
                           objetivo, "OK" if ok else "FALLA"))

    # 1. Pago oportuno
    vencidos = aportes[aportes["EstadoPago"] != "Pendiente"]
    v = (vencidos["EstadoPago"] == "Oportuno").mean()
    meta(1, "Pago oportuno", v, 0.78 <= v <= 0.82, "80% ±2 pp", "{:.1%}")

    # 2. Construcción y transporte: tasa de mora > 30 días / promedio
    for sector in ("Construcción", "Transporte"):
        v = mora.loc[mora["ActividadEconomica"] == sector, "mora30"].mean() / tasa
        meta(2, f"Mora {sector} / promedio", v, 1.7 <= v <= 2.3, "1,7–2,3×")

    # 3 y 4. Antigüedad
    v = mora.loc[mora["anios"] > 10, "mora30"].mean() / tasa
    meta(3, "Mora empresas > 10 años / promedio", v, v <= 0.7, "≤ 0,7×")
    v = mora.loc[mora["anios"] < 2, "mora30"].mean() / tasa
    meta(4, "Mora empresas < 2 años / promedio", v, v >= 1.3, "≥ 1,3×")

    # 5. Cotizantes vs valor esperado (Spearman = Pearson sobre rangos)
    v = aportes["NumeroCotizantes"].rank().corr(aportes["ValorEsperado"].rank())
    meta(5, "Spearman cotizantes vs esperado", v, v >= 0.85, "≥ 0,85")

    # 6. Recaudo de medianas y grandes
    ap = aportes.join(info["TamanoEmpresa"], on="AportanteID")
    v = ap.loc[ap["TamanoEmpresa"].isin(["Mediana", "Grande"]), "ValorPagado"].sum() / ap["ValorPagado"].sum()
    meta(6, "% recaudo de medianas + grandes", v, 0.45 <= v <= 0.55, "45–55%", "{:.1%}")

    # 7. Persistencia de la mora
    m = mora.sort_values(["AportanteID", "Periodo"])
    sig = m.groupby("AportanteID")["DiasMora"].shift(-1)
    en_mora = m["DiasMora"] > 0
    ok_sig = sig.notna()
    p11 = (sig[en_mora & ok_sig] > 0).mean()
    p01 = (sig[~en_mora & ok_sig] > 0).mean()
    meta(7, "P(mora t+1 | mora t) vs P(mora t+1 | al día t)", f"{p11:.2f} vs {p01:.2f}",
         p11 >= 0.5 and p01 <= 0.10, "≥ 0,50 vs ≤ 0,10")

    # 8. AUC del score por reglas contra mora > 30 días en t+1
    r = riesgo[riesgo["ScoreVersion"] == "reglas-v1"].copy()
    r["Periodo"] = r["FechaEvaluacion"].str[:7]
    m["mora30_sig"] = m.groupby("AportanteID")["mora30"].shift(-1)
    x = r.merge(m[["AportanteID", "Periodo", "mora30_sig"]], on=["AportanteID", "Periodo"])
    x = x[x["mora30_sig"].notna()]
    v = auc(x["ProbabilidadIncumplimiento"].to_numpy(), x["mora30_sig"].astype(bool).to_numpy())
    meta(8, "AUC score por reglas (mora > 30 en t+1)", v, 0.80 <= v <= 0.88, "0,80–0,88")

    # 9. Gestiones por aportante-mes según el tramo del cierre anterior
    cob["Periodo"] = cob["FechaGestion"].str[:7]
    n_ges = cob.groupby(["AportanteID", "Periodo"]).size().rename("n")
    m["Periodo_sig"] = m.groupby("AportanteID")["Periodo"].shift(-1)
    g = m[m["Periodo_sig"].notna()].join(n_ges, on=["AportanteID", "Periodo_sig"])
    prom = g["n"].fillna(0).groupby(g["EstadoMora"]).mean().reindex(TRAMOS)
    creciente = bool(np.all(np.diff(prom.to_numpy()) > 0))
    meta(9, "Gestiones/mes por tramo", " · ".join(f"{v:.1f}" for v in prom), creciente,
         "creciente por tramo")

    # 10. Acuerdos: saldo 6 meses después de firmar
    acu = leer(datos, "FactAcuerdoPago")
    firmas = acu.drop_duplicates("AcuerdoID")[["AcuerdoID", "AportanteID", "FechaAcuerdo", "EstadoAcuerdo"]]
    firmas["P0"] = firmas["FechaAcuerdo"].str[:7]
    firmas["P6"] = (pd.to_datetime(firmas["P0"] + "-01") + pd.DateOffset(months=6)).dt.strftime("%Y-%m")
    saldo = mora.set_index(["AportanteID", "Periodo"])["SaldoTotal"]
    firmas = firmas[firmas["P6"] <= mora["Periodo"].max()]
    s0 = saldo.reindex(list(zip(firmas["AportanteID"], firmas["P0"]))).to_numpy()
    s6 = saldo.reindex(list(zip(firmas["AportanteID"], firmas["P6"]))).fillna(0).to_numpy()
    cumplidos = (firmas["EstadoAcuerdo"] == "Cumplido").to_numpy()
    v_acu = s6[cumplidos].sum() / s0[cumplidos].sum() - 1
    # comparación: aportantes en mora > 30 sin acuerdo
    base = m[(m["DiasMora"] > 30) & ~m["TieneAcuerdo"].astype(bool)]
    base_p6 = (pd.to_datetime(base["Periodo"] + "-01") + pd.DateOffset(months=6)).dt.strftime("%Y-%m")
    b6 = saldo.reindex(list(zip(base["AportanteID"], base_p6))).fillna(0).to_numpy()
    sel = base_p6.to_numpy() <= mora["Periodo"].max()
    v_sin = b6[sel].sum() / base["SaldoTotal"].to_numpy()[sel].sum() - 1
    meta(10, "Saldo a 6 meses: acuerdo cumplido vs sin acuerdo", f"{v_acu:+.0%} vs {v_sin:+.0%}",
         v_acu <= -0.40, "con acuerdo ≤ −40%")

    # 11. Salarios
    anio = pd.to_datetime(dim_cot["FechaIngreso"]).dt.year.clip(2023, 2025)
    x = dim_cot["Salario"] / anio.map(smmlv)
    v1, v2 = ((x >= 1) & (x <= 1.5)).mean(), (x > 10).mean()
    meta(11, "Salario 1–1,5 SMMLV · > 10 SMMLV", f"{v1:.0%} · {v2:.1%}",
         abs(v1 - 0.55) <= 0.05 and v2 <= 0.05, "55% ±5 pp · ≤ 5%")

    # 12. Distribución de riesgo al último corte
    ult = r[r["FechaEvaluacion"] == r["FechaEvaluacion"].max()]["NivelRiesgo"].value_counts(normalize=True)
    dist = ult.reindex(["Bajo", "Medio", "Alto", "Critico"]).fillna(0).to_numpy()
    objetivo = np.array([0.60, 0.25, 0.10, 0.05])
    meta(12, "Riesgo Bajo · Medio · Alto · Crítico", " · ".join(f"{v:.0%}" for v in dist),
         bool(np.all(np.abs(dist - objetivo) <= 0.05)), "60 · 25 · 10 · 5 (±5 pp)")

    ancho = max(len(r[1]) for r in resultados)
    print(f"\n{'#':>3}  {'Meta':<{ancho}}  {'Valor':>18}  {'Objetivo':>22}  Estado")
    print("-" * (ancho + 58))
    for n, nombre, valor, obj, estado in resultados:
        print(f"{n:>3}  {nombre:<{ancho}}  {valor:>18}  {obj:>22}  {estado}")
    fallas = sum(r[4] == "FALLA" for r in resultados)
    print("-" * (ancho + 58))
    print(f"Tasa de mora > 30 días promedio: {tasa:.1%} de los aportante-mes")
    print("Todas las metas se cumplen." if not fallas else f"{fallas} meta(s) fuera de tolerancia.")
    return fallas == 0


def main(argv=None):
    ap = argparse.ArgumentParser(description="Valida las metas de correlación del dataset.")
    ap.add_argument("--datos", default="data/raw")
    ap.add_argument("--config", default="config/parametros.yaml")
    args = ap.parse_args(argv)
    sys.exit(0 if validar(args.datos, args.config) else 1)


if __name__ == "__main__":
    main()
