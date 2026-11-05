"""Generador de datos sintéticos: recaudo y cartera de un fondo de pensiones.

Produce CSV relacionados entre sí (dimensiones y hechos) para construir una
plataforma de analítica end-to-end en Databricks (y luego en Microsoft Fabric).

Cómo funciona, en una frase: cada empresa recibe un "riesgo latente" oculto
(sector x tamaño x antigüedad x ruido) y una simulación mes a mes decide, a
partir de ese riesgo y de su historia, si paga a tiempo, tarde, parcial o no
paga; la deuda se acumula, genera intereses, dispara gestiones de cobranza y
a veces acuerdos de pago que la reducen cuota a cuota.

Uso local:
    python src/generador/generar.py --config config/parametros.yaml --salida data/raw
    python src/generador/generar.py --escala 0.1          # 10% del tamaño, para iterar
    python src/generador/generar.py --particionar         # un archivo por mes (Auto Loader)

Uso en un notebook (Databricks o Fabric): sube la carpeta src/generador y config/,
y ejecuta:
    from generar import generar
    generar("config/parametros.yaml", "/Volumes/recaudo_dev/raw/landing")        # Databricks
    generar("config/parametros.yaml", "/lakehouse/default/Files/raw")            # Fabric
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import date
from pathlib import Path

import holidays
import numpy as np
import pandas as pd
import yaml
from faker import Faker

try:  # funciona como paquete y como script suelto / notebook
    from . import catalogos as cat
except ImportError:  # pragma: no cover
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import catalogos as cat

EPOCH = date(1970, 1, 1).toordinal()
TRAMOS = ["Sin Mora", "1-30", "31-60", "61-90", ">90"]
MESES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto",
         "Septiembre", "Octubre", "Noviembre", "Diciembre"]
DIAS = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]

# Códigos internos de resultado del periodo (decisión inicial al vencer)
OPORTUNO, TARDIO, PARCIAL, NO_PAGO = 1, 2, 3, 4


# =============================================================================
# Utilidades de fechas
# =============================================================================
def ord_a_iso(ords: np.ndarray) -> np.ndarray:
    """Convierte ordinales (int) a 'yyyy-MM-dd'. Valores <= 0 quedan vacíos."""
    ords = np.asarray(ords, dtype=np.int64)
    out = (ords - EPOCH).astype("datetime64[D]").astype(str).astype(object)
    out[ords <= 0] = None
    return out


def ord_partes(ords: np.ndarray):
    """Año, mes y día de un arreglo de ordinales."""
    d = (np.asarray(ords, dtype=np.int64) - EPOCH).astype("datetime64[D]")
    meses = d.astype("datetime64[M]")
    anio = meses.astype("datetime64[Y]").astype(int) + 1970
    mes = meses.astype(int) % 12 + 1
    dia = (d - meses.astype("datetime64[D]")).astype(int) + 1
    return anio, mes, dia


class Calendario:
    """Meses internos de la simulación (índice 0 = primer mes de calentamiento).

    El periodo p se paga en el mes p+1 (mes vencido) con fecha límite en el
    k-ésimo día hábil, según los dos últimos dígitos del NIT.
    """

    def __init__(self, cfg):
        f = cfg["fechas"]
        y0, m0 = map(int, f["inicio"].split("-"))
        y1, m1 = map(int, f["fin"].split("-"))
        self.warmup = int(f["meses_calentamiento"])
        base = y0 * 12 + (m0 - 1) - self.warmup
        self.base = base
        self.out_first = self.warmup
        self.out_last = (y1 * 12 + m1 - 1) - base
        self.n_meses = self.out_last + 2          # +1 mes para la fecha límite del último periodo
        self.corte = date.fromisoformat(f["corte"]).toordinal()
        self.inicio_salida = date(y0, m0, 1).toordinal()

        anios = range(base // 12, (base + self.n_meses) // 12 + 1)
        festivos = holidays.country_holidays("CO", years=list(anios))
        self.inicio_mes, self.fin_mes, self.habiles, self.label = [], [], [], []
        todos_habiles = []
        for m in range(self.n_meses):
            y, mo = divmod(base + m, 12)
            mo += 1
            ini = date(y, mo, 1)
            fin = date(y + (mo == 12), mo % 12 + 1, 1).toordinal() - 1
            hab = [o for o in range(ini.toordinal(), fin + 1)
                   if date.fromordinal(o).weekday() < 5 and date.fromordinal(o) not in festivos]
            self.inicio_mes.append(ini.toordinal())
            self.fin_mes.append(fin)
            self.habiles.append(np.array(hab, dtype=np.int64))
            self.label.append(f"{y}-{mo:02d}")
            todos_habiles.extend(hab)
        self.todos_habiles = np.array(todos_habiles, dtype=np.int64)
        self.festivos = festivos
        self.anio_mes = np.array([(base + m) // 12 for m in range(self.n_meses)])

    def siguiente_habil(self, o: int) -> int:
        i = np.searchsorted(self.todos_habiles, o)
        return int(self.todos_habiles[min(i, len(self.todos_habiles) - 1)])

    def mes_de(self, ords: np.ndarray) -> np.ndarray:
        anio, mes, _ = ord_partes(ords)
        return anio * 12 + mes - 1 - self.base


def dia_habil_por_nit(nit: np.ndarray) -> np.ndarray:
    """Día hábil de pago PILA según los dos últimos dígitos del NIT (2.º a 16.º)."""
    limites = np.array([8, 15, 22, 29, 36, 43, 50, 57, 64, 70, 76, 82, 88, 94, 100])
    return np.searchsorted(limites, nit % 100, side="right") + 2


def digito_verificacion(nit: int) -> int:
    """Dígito de verificación del NIT (algoritmo DIAN, módulo 11)."""
    pesos = [3, 7, 13, 17, 19, 23, 29, 37, 41, 43, 47, 53, 59, 67, 71]
    s = sum(int(c) * pesos[i] for i, c in enumerate(reversed(str(nit))))
    r = s % 11
    return r if r in (0, 1) else 11 - r


def elegir(rng, opciones: dict, n: int):
    claves = list(opciones)
    p = np.array([opciones[k] for k in claves], dtype=float)
    return np.array(claves, dtype=object)[rng.choice(len(claves), n, p=p / p.sum())]


# =============================================================================
# Dimensiones
# =============================================================================
def generar_geografia():
    filas = []
    for i, (ciudad, depto, dane, pob) in enumerate(cat.CIUDADES, start=1):
        filas.append({"GeografiaID": i,
                      "Regional": cat.REGIONAL_POR_DEPTO.get(depto, "Centro Oriente"),
                      "Departamento": depto, "CodigoDANE": dane, "Ciudad": ciudad,
                      "_pob": pob})
    geo = pd.DataFrame(filas)
    pesos = geo["_pob"].to_numpy(dtype=float, copy=True)
    es_bog = (geo["Ciudad"] == "Bogotá").to_numpy()
    resto = pesos[~es_bog].sum()
    pesos[es_bog] = resto * cat.PCT_APORTANTES_BOGOTA / (1 - cat.PCT_APORTANTES_BOGOTA)
    return geo.drop(columns="_pob"), pesos / pesos.sum()


def generar_aportantes(cfg, rng, fake, cal, pesos_geo, n_geo):
    """Empresas: atributos, riesgo latente, eventos de vida (cierre, liquidación, traslado)."""
    n = int(round(cfg["poblacion"]["n_aportantes"] * cfg["escala"]))
    tam_cfg, sec_cfg = cfg["tamanos"], cfg["sectores"]
    tam_nombres, sec_nombres = list(tam_cfg), list(sec_cfg)

    tam = rng.choice(len(tam_nombres), n, p=[tam_cfg[t]["peso"] for t in tam_nombres])
    sec = rng.choice(len(sec_nombres), n, p=[sec_cfg[s]["peso"] for s in sec_nombres])
    geo = rng.choice(n_geo, n, p=pesos_geo) + 1

    # Fecha de afiliación: la mayoría antes de 2023 (sesgo a fechas recientes),
    # un % se afilia durante la ventana publicada.
    af = cfg["afiliacion"]
    desde = date.fromisoformat(af["desde"]).toordinal()
    nuevas = rng.random(n) < af["pct_nuevas_en_ventana"]
    anios = np.exp(rng.normal(math.log(af["mediana_anios"]), 0.8, n))
    aff = (cal.inicio_salida - anios * 365.25).astype(np.int64)
    aff = np.maximum(aff, desde)
    fin_nuevas = date(2025, 9, 30).toordinal()
    aff[nuevas] = rng.integers(cal.inicio_salida, fin_nuevas, nuevas.sum())

    # Número de puestos (cotizantes simultáneos) según tamaño.
    h = np.zeros(n, dtype=np.int64)
    for t, nombre in enumerate(tam_nombres):
        idx = np.nonzero(tam == t)[0]
        c = tam_cfg[nombre]
        if nombre == "Micro":
            v = rng.geometric(0.33, len(idx))
            while (v > c["max"]).any():
                malos = v > c["max"]
                v[malos] = rng.geometric(0.33, malos.sum())
        elif nombre == "Pequeña":
            v = c["min"] + np.floor(rng.beta(1.5, 3, len(idx)) * (c["max"] - c["min"] + 1))
        elif nombre == "Mediana":
            v = c["min"] + np.floor(rng.beta(1.2, 3.2, len(idx)) * (c["max"] - c["min"] + 1))
        else:
            v = c["min"] + np.floor(np.exp(rng.normal(3.55, 0.8, len(idx))))
        h[idx] = np.clip(v, c["min"], c["max"])
    # Ajuste fino para llegar al objetivo de cotizantes: la diferencia va a las grandes.
    objetivo = int(round(cfg["poblacion"]["n_cotizantes_objetivo"] * cfg["escala"]))
    grandes = np.nonzero(tam == tam_nombres.index("Grande"))[0]
    if len(grandes):
        dif = objetivo - h.sum()
        reparto = np.floor(dif * h[grandes] / h[grandes].sum()).astype(np.int64)
        h[grandes] = np.clip(h[grandes] + reparto, tam_cfg["Grande"]["min"], tam_cfg["Grande"]["max"])

    # Riesgo latente: sector x tamaño x ruido (la antigüedad se aplica mes a mes).
    sigma = cfg["ruido_riesgo_sigma"]
    ruido = np.exp(rng.normal(-sigma ** 2 / 2, sigma, n))
    base_sec = np.array([sec_cfg[s]["riesgo_base"] for s in sec_nombres])[sec]
    f_tam = np.array([tam_cfg[t]["factor_riesgo"] for t in tam_nombres])[tam]
    riesgo = base_sec * f_tam * ruido * cfg["pago"]["calibracion"]

    # Eventos de vida dentro de la ventana.
    ev = cfg["eventos_aportante"]
    ini_ev = np.maximum(aff, cal.inicio_salida) + 180
    fin_ev = cal.corte - 60
    puede = ini_ev < fin_ev
    u = rng.random(n)
    # La liquidación es más probable en empresas riesgosas.
    p_liq = ev["pct_liquidacion"] * riesgo / riesgo.mean()
    liquidacion = puede & (u < p_liq)
    inactivo = puede & ~liquidacion & (rng.random(n) < ev["pct_inactivo"])
    fecha_liq = np.full(n, 10 ** 7, dtype=np.int64)
    fin_emp = np.full(n, 10 ** 7, dtype=np.int64)          # fecha en que ya no hay trabajadores
    for mask, es_liq in ((liquidacion, True), (inactivo, False)):
        idx = np.nonzero(mask)[0]
        f = (ini_ev[idx] + rng.random(len(idx)) * (fin_ev - ini_ev[idx])).astype(np.int64)
        if es_liq:
            fecha_liq[idx] = f
            fin_emp[idx] = f + rng.integers(30, 91, len(idx))
        else:
            fin_emp[idx] = f
    traslado = puede & (rng.random(n) < ev["pct_traslado"])
    fecha_tras = (ini_ev + rng.random(n) * np.maximum(fin_ev - ini_ev, 1)).astype(np.int64)
    geo_nuevo = rng.choice(n_geo, n, p=pesos_geo) + 1

    # NIT único y razón social.
    nit = 800_000_000 + rng.choice(200_000_000, n, replace=False)
    dv = np.array([digito_verificacion(int(x)) for x in nit])
    usados, razon = set(), []
    for s in sec:
        pref = cat.PREFIJOS[sec_nombres[s]]
        for _ in range(20):
            r = rng.random()
            nucleo = fake.last_name() if r < 0.6 else (
                f"{fake.last_name()} & {fake.last_name()}" if r < 0.85 else fake.city())
            nombre = f"{pref[rng.integers(len(pref))]} {nucleo} {cat.SUFIJOS_SOCIEDAD[rng.integers(len(cat.SUFIJOS_SOCIEDAD))]}"
            if nombre not in usados:
                break
        usados.add(nombre)
        razon.append(nombre)
    ciiu = np.array([cat.CIIU[sec_nombres[s]][rng.integers(len(cat.CIIU[sec_nombres[s]]))] for s in sec])

    return {
        "n": n, "tam": tam, "sec": sec, "geo": geo, "aff": aff, "h": h, "riesgo": riesgo,
        "fecha_liq": fecha_liq, "fin_emp": fin_emp, "liquidacion": liquidacion,
        "inactivo": inactivo, "traslado": traslado, "fecha_tras": fecha_tras,
        "geo_nuevo": geo_nuevo, "nit": nit, "dv": dv, "razon": np.array(razon, dtype=object),
        "ciiu": ciiu, "tam_nombres": tam_nombres, "sec_nombres": sec_nombres,
        "id": np.array([f"APT-{i + 1:06d}" for i in range(n)], dtype=object),
    }


def tabla_dim_aportante(A, cal):
    """DimAportante con una fila por versión (formato CDC para AUTO CDC en Silver)."""
    base = pd.DataFrame({
        "_i": np.arange(A["n"]), "AportanteID": A["id"], "Nit": A["nit"].astype(str),
        "DigitoVerificacion": A["dv"], "RazonSocial": A["razon"], "GeografiaID": A["geo"],
        "ActividadEconomica": np.array(A["sec_nombres"], dtype=object)[A["sec"]],
        "CodigoCIIU": A["ciiu"],
        "TamanoEmpresa": np.array(A["tam_nombres"], dtype=object)[A["tam"]],
        "FechaAfiliacion": A["aff"], "EstadoAportante": "Activo",
        "RiesgoLatente": np.round(A["riesgo"], 5), "FechaActualizacion": A["aff"],
    })
    versiones = [base]
    t = A["traslado"]
    if t.any():
        v = base[t].copy()
        v["GeografiaID"] = A["geo_nuevo"][t]
        v["FechaActualizacion"] = A["fecha_tras"][t]
        versiones.append(v)
    for mask, estado, fecha in ((A["liquidacion"], "En liquidación", A["fecha_liq"]),
                                (A["inactivo"], "Inactivo", A["fin_emp"])):
        if mask.any():
            v = base[mask].copy()
            v["EstadoAportante"] = estado
            v["FechaActualizacion"] = fecha[mask]
            versiones.append(v)
    df = pd.concat(versiones).sort_values(["_i", "FechaActualizacion"])
    # Cada versión arrastra los cambios anteriores: una empresa trasladada y luego
    # inactiva conserva su nueva ciudad.
    i = df["_i"].to_numpy()
    ya_trasladada = A["traslado"][i] & (df["FechaActualizacion"].to_numpy() >= A["fecha_tras"][i])
    df["GeografiaID"] = np.where(ya_trasladada, A["geo_nuevo"][i], A["geo"][i])
    df["FechaAfiliacion"] = ord_a_iso(df["FechaAfiliacion"].to_numpy())
    df["FechaActualizacion"] = ord_a_iso(df["FechaActualizacion"].to_numpy())
    return df.drop(columns="_i").reset_index(drop=True)


def generar_cotizantes(cfg, rng, A, cal):
    """Puestos de trabajo -> trabajadores, con rotación (retiro + reemplazo)."""
    cc = cfg["cotizantes"]
    pens = cfg["pension"]
    slot_ap = np.repeat(np.arange(A["n"]), A["h"])
    ns = len(slot_ap)
    aff = A["aff"][slot_ap]
    fin_emp = A["fin_emp"][slot_ap]
    tope = np.minimum(fin_emp, cal.corte)

    def edad_y_nacimiento(k):
        e = rng.normal(cc["edad_media"], cc["edad_desv"], k)
        while ((e < 18) | (e > 65)).any():
            m = (e < 18) | (e > 65)
            e[m] = rng.normal(cc["edad_media"], cc["edad_desv"], m.sum())
        nac = (cal.corte - np.floor(e) * 365.25 - rng.integers(0, 365, k)).astype(np.int64)
        return nac

    # Trabajador inicial de cada puesto.
    antes = aff < cal.inicio_salida
    ini = np.where(
        antes,
        np.maximum(aff, cal.inicio_salida - 3650) + (rng.random(ns) * (
            cal.inicio_salida - np.maximum(aff, cal.inicio_salida - 3650))).astype(np.int64),
        aff + rng.integers(0, 61, ns))
    nac = edad_y_nacimiento(ns)
    ini = np.maximum(ini, nac + 6575)                 # al menos 18 años al ingresar
    fin = np.full(ns, 0, dtype=np.int64)              # 0 = sigue activo
    fin = np.where(fin_emp < 10 ** 7, fin_emp, 0)

    # Rotación: un % de puestos tiene un retiro y un reemplazo dentro de la ventana.
    rot_lo = np.maximum(ini, cal.inicio_salida) + 30
    rot_hi = tope - 30
    rota = (rng.random(ns) < cc["rotacion"]) & (rot_lo < rot_hi)
    retiro = (rot_lo + rng.random(ns) * np.maximum(rot_hi - rot_lo, 1)).astype(np.int64)
    fin = np.where(rota, retiro, fin)
    ini2 = retiro + rng.integers(1, 46, ns)
    rota &= ini2 < tope
    k2 = rota.sum()
    nac2 = edad_y_nacimiento(k2)
    ini2 = np.maximum(ini2[rota], nac2 + 6575)
    fin2 = np.where(fin_emp[rota] < 10 ** 7, fin_emp[rota], 0)
    ok2 = ini2 < tope[rota]

    ap = np.concatenate([slot_ap, slot_ap[rota][ok2]])
    ini = np.concatenate([ini, ini2[ok2]])
    fin = np.concatenate([fin, fin2[ok2]])
    nac = np.concatenate([nac, nac2[ok2]])
    valido = (ini <= cal.corte) & ((fin == 0) | (fin > ini))
    ap, ini, fin, nac = ap[valido], ini[valido], fin[valido], nac[valido]
    n = len(ap)

    # Salario como múltiplo del SMMLV: ~30% gana el mínimo exacto, una franja
    # entre 1 y 1,5 y una cola lognormal; las empresas grandes pagan más.
    s = cc["salario"]
    u = rng.random(n)
    f_sal = np.array([cfg["tamanos"][t]["factor_salario"] for t in A["tam_nombres"]])[A["tam"][ap]]
    cola = 1.5 + np.exp(rng.normal(s["cola_mu"] + f_sal, s["cola_sigma"], n))
    x = np.where(u < s["pct_minimo_exacto"], 1.0,
                 np.where(u < s["pct_minimo_exacto"] + s["pct_hasta_1_5"],
                          1 + rng.random(n) * 0.5, np.minimum(cola, s["max_smmlv"])))

    anio_ini, _, _ = ord_partes(ini)
    anio_ref = np.clip(anio_ini, 2023, 2025)
    smmlv = pens["smmlv"]
    smm_ref = np.array([smmlv[int(a)] for a in anio_ref])
    salario = np.where(x == 1.0, smm_ref, np.round(x * smm_ref, -2))

    tipo_doc = elegir(rng, cc["tipo_documento"], n)
    num = np.zeros(n, dtype=np.int64)
    rangos = {"CC": (10_000_000, 1_200_000_000), "CE": (100_000, 9_999_999), "PPT": (1_000_000, 9_999_999)}
    for t, (lo, hi) in rangos.items():
        idx = np.nonzero(tipo_doc == t)[0]
        v = rng.choice(hi - lo, len(idx), replace=False) + lo
        num[idx] = v

    return {
        "n": n, "ap": ap, "ini": ini, "fin": fin, "nac": nac, "x": x,
        "salario": salario, "anio_ref": anio_ref,
        "tipo_doc": tipo_doc, "num_doc": num,
        "genero": np.where(rng.random(n) < cc["pct_genero_m"], "M", "F"),
        "tipo_cot": elegir(rng, cc["tipo_cotizante"], n),
        "id": np.array([f"COT-{i + 1:07d}" for i in range(n)], dtype=object),
    }


def tabla_dim_cotizante(C, A, cal):
    anio_n, mes_n, dia_n = ord_partes(C["nac"])
    anio_c, mes_c, dia_c = ord_partes(np.array([cal.corte]))
    edad = anio_c[0] - anio_n - ((mes_n > mes_c[0]) | ((mes_n == mes_c[0]) & (dia_n > dia_c[0])))
    return pd.DataFrame({
        "CotizanteID": C["id"], "AportanteID": A["id"][C["ap"]],
        "TipoDocumento": C["tipo_doc"], "NumeroDocumento": C["num_doc"].astype(str),
        "Genero": C["genero"], "FechaNacimiento": ord_a_iso(C["nac"]), "Edad": edad,
        "Salario": C["salario"].astype(np.int64), "TipoCotizante": C["tipo_cot"],
        "FechaIngreso": ord_a_iso(C["ini"]), "FechaRetiro": ord_a_iso(C["fin"]),
    })


def tabla_dim_tiempo(cal):
    ini = cal.inicio_salida
    fin = cal.fin_mes[-1]                  # incluye el mes de la última fecha límite
    o = np.arange(ini, fin + 1)
    anio, mes, dia = ord_partes(o)
    d = (o - EPOCH).astype("datetime64[D]")
    dow = ((d.astype(np.int64) + 3) % 7)  # 0 = lunes
    iso = pd.to_datetime(d).isocalendar()
    festivo = np.array([date.fromordinal(int(x)) in cal.festivos for x in o])
    return pd.DataFrame({
        "Fecha": ord_a_iso(o), "FechaKey": anio * 10000 + mes * 100 + dia,
        "Anio": anio, "Mes": mes, "NombreMes": np.array(MESES, dtype=object)[mes - 1],
        "Trimestre": (mes - 1) // 3 + 1, "Semana": iso["week"].to_numpy(), "Dia": dia,
        "DiaSemana": dow + 1, "NombreDia": np.array(DIAS, dtype=object)[dow],
        "EsFestivo": festivo, "EsDiaHabil": (dow < 5) & ~festivo,
        "Periodo": [f"{a}-{m:02d}" for a, m in zip(anio, mes)],
    })


# =============================================================================
# Detalle de aportes (cotizante x periodo) y valor esperado
# =============================================================================
def generar_detalle(cfg, C, A, cal):
    """Una fila por cotizante y periodo publicado: IBC, cotización y FSP."""
    pens = cfg["pension"]
    m_ini = cal.mes_de(C["ini"])
    _, _, d_ini = ord_partes(C["ini"])
    fin_ord = np.where(C["fin"] > 0, C["fin"], 10 ** 7)
    m_fin = np.where(C["fin"] > 0, cal.mes_de(np.where(C["fin"] > 0, C["fin"], cal.corte)), 10 ** 6)
    _, _, d_fin = ord_partes(np.where(C["fin"] > 0, C["fin"], cal.corte))

    s = np.maximum(m_ini, cal.out_first)
    e = np.minimum(m_fin, cal.out_last)
    ok = s <= e
    idx = np.nonzero(ok)[0]
    n_per = (e[idx] - s[idx] + 1)
    fila = np.repeat(idx, n_per)
    periodo = np.concatenate([np.arange(a, b + 1) for a, b in zip(s[idx], e[idx])])

    # Días cotizados: mes completo = 30; ajustes en el mes de ingreso y de retiro.
    dias = np.full(len(fila), 30, dtype=np.int64)
    es_ini = periodo == m_ini[fila]
    es_fin = periodo == m_fin[fila]
    dias = np.where(es_ini, np.clip(31 - d_ini[fila], 1, 30), dias)
    dias = np.where(es_fin & ~es_ini, np.clip(d_fin[fila], 1, 30), dias)
    dias = np.where(es_fin & es_ini, np.clip(d_fin[fila] - d_ini[fila] + 1, 1, 30), dias)

    ultimo_anio = max(pens["smmlv"])          # meses posteriores usan el último SMMLV conocido
    smm_mes = np.array([pens["smmlv"][min(int(a), ultimo_anio)] for a in cal.anio_mes])
    smm = smm_mes[periodo]
    x = C["x"][fila]
    ibc = np.where(x == 1.0, smm, np.round(x * smm, -2)) * dias / 30
    ibc = np.clip(ibc, pens["ibc_min_smmlv"] * smm * dias / 30, pens["ibc_max_smmlv"] * smm)
    ibc = np.round(ibc)
    cotiz = np.round(ibc * pens["tarifa"])
    ratio = ibc / smm
    pct_fsp = np.zeros(len(ibc))
    for tramo in pens["fsp"]:
        pct_fsp = np.where(ratio >= tramo["desde"], tramo["pct"], pct_fsp)
    fsp = np.round(ibc * pct_fsp)

    ap = C["ap"][fila]
    det = pd.DataFrame({
        "Periodo": np.array(cal.label, dtype=object)[periodo],
        "AportanteID": A["id"][ap], "CotizanteID": C["id"][fila],
        "DiasCotizados": dias, "IBC": ibc.astype(np.int64),
        "TarifaPension": pens["tarifa"], "ValorCotizacion": cotiz.astype(np.int64),
        "ValorFSP": fsp.astype(np.int64), "ValorEsperado": (cotiz + fsp).astype(np.int64),
    })

    # Matriz de valor esperado y número de cotizantes por aportante x mes interno.
    E = np.zeros((A["n"], cal.n_meses), dtype=np.float64)
    N = np.zeros((A["n"], cal.n_meses), dtype=np.int64)
    np.add.at(E, (ap, periodo), (cotiz + fsp))
    np.add.at(N, (ap, periodo), 1)
    # Calentamiento: los meses previos se aproximan con el primer periodo
    # publicado, deflactado por el SMMLV del año anterior.
    deflactor = pens["smmlv"][int(cal.anio_mes[0])] / pens["smmlv"][int(cal.anio_mes[cal.out_first])]
    m_aff = cal.mes_de(A["aff"])
    for p in range(cal.out_first):
        activos = (m_aff <= p) & (E[:, cal.out_first] > 0)
        E[activos, p] = np.round(E[activos, cal.out_first] * deflactor)
        N[activos, p] = N[activos, cal.out_first]
    return det, E, N


# =============================================================================
# Simulación mensual de pagos, mora, cobranza, acuerdos y riesgo
# =============================================================================
def simular(cfg, rng, fake, A, E, cal):
    pg, cb, ac, rr = cfg["pago"], cfg["cobranza"], cfg["acuerdos"], cfg["riesgo_reglas"]
    n = A["n"]
    dr = (1 + cfg["mora"]["tasa_efectiva_anual"]) ** (1 / 365) - 1

    # Fecha límite de cada periodo p (se paga en el mes p+1).
    k = dia_habil_por_nit(A["nit"])
    limite = np.zeros((n, cal.n_meses), dtype=np.int64)
    for p in range(cal.n_meses - 1):
        hab = cal.habiles[p + 1]
        limite[:, p] = hab[np.minimum(k - 1, len(hab) - 1)]

    # Factor de antigüedad por mes interno (la empresa envejece).
    edades = (np.array(cal.fin_mes)[None, :] - A["aff"][:, None]) / 365.25
    f_ant = np.ones_like(edades)
    for tramo in reversed(cfg["antiguedad_factor"]):
        f_ant = np.where(edades < tramo["hasta_anios"], tramo["factor"], f_ant)
    r_mes = np.clip(A["riesgo"][:, None] * f_ant, 0.001, 0.9)
    r_ref = float(np.median(A["riesgo"]))

    sector_alto = np.isin(np.array(A["sec_nombres"])[A["sec"]], ["Construcción", "Transporte"])
    es_micro = np.array(A["tam_nombres"])[A["tam"]] == "Micro"
    rango_dias = pg["tardio_dias"]
    canales_pila, canales_abono = pg["canal_pila"], pg["canal_abono"]
    lam = np.array([0.0] + [cb["lambda_tramo"][t] for t in TRAMOS[1:]])
    mezcla = {t: cb["mezcla_tipo"][t] for t in TRAMOS[1:]}
    nombres = [fake.name() for _ in range(1500)]
    gestores = [fake.name() for _ in range(cb["n_gestores"])]

    # Estado: deuda como lista de "tramos" por periodo:
    # [periodo, fecha_limite, capital, interes_congelado(-1 = se causa), acuerdo_id, fecha_programada]
    deuda = [[] for _ in range(n)]
    acuerdo_activo = np.zeros(n, dtype=np.int64)
    acuerdos = {}
    resultado = np.zeros((n, cal.n_meses), dtype=np.int8)
    tramo_prev = np.zeros(n, dtype=np.int64)
    pagos, gestiones, mora_filas, riesgo_filas = [], [], [], []

    def interes(t, hoy):
        return t[3] if t[3] >= 0 else t[2] * dr * max(hoy - t[1], 0)

    def registrar_pago(a, t, fecha, cap, intr, tipo, acuerdo_id=0):
        if fecha < cal.inicio_salida or fecha > cal.corte:
            return
        canal = elegir(rng, canales_abono if tipo == "Abono acuerdo" else canales_pila, 1)[0]
        pagos.append((a, t, fecha, int(round(cap)), int(round(intr)), tipo, canal, acuerdo_id))

    def pagar_fifo(a, monto, fecha, tipo, filtro, acuerdo_id=0):
        """Aplica un pago a los tramos más antiguos que cumplen 'filtro'."""
        for t in sorted((t for t in deuda[a] if filtro(t)), key=lambda t: t[1]):
            if monto <= 0.5:
                break
            i = interes(t, fecha)
            total = t[2] + i
            if monto >= total - 0.5:
                registrar_pago(a, t[0], fecha, t[2], i, tipo, acuerdo_id)
                monto -= total
                t[2] = 0.0
            else:
                frac = monto / total
                cap_p, int_p = t[2] * frac, i * frac
                registrar_pago(a, t[0], fecha, cap_p, int_p, tipo, acuerdo_id)
                t[2] -= cap_p
                if t[3] >= 0:
                    t[3] -= int_p
                monto = 0
        deuda[a] = [t for t in deuda[a] if t[2] > 0.5]

    def libre(t):
        return t[4] == 0 and t[5] == 0

    for M in range(1, cal.out_last + 1):
        ms, me, hab = cal.inicio_mes[M], cal.fin_mes[M], cal.habiles[M]
        publicar = M >= cal.out_first
        exito = np.zeros(n, dtype=bool)

        # ---- 1. Gestiones de cobranza (más mora -> más gestiones) -------------
        if publicar:
            cand = np.nonzero(tramo_prev > 0)[0]
            cuantas = rng.poisson(lam[tramo_prev[cand]])
            for a, q in zip(cand, cuantas):
                if q == 0:
                    continue
                tr = TRAMOS[tramo_prev[a]]
                fechas = np.sort(rng.choice(hab, q))
                tipos = elegir(rng, mezcla[tr], q)
                liq = A["fecha_liq"][a] <= ms
                for f, tipo in zip(fechas, tipos):
                    if tipo == "AcuerdoPago" and acuerdo_activo[a]:
                        tipo = "Llamada"
                    probs = dict(cb["resultados"][tipo])
                    ajuste = float(np.clip((r_ref / A["riesgo"][a]) ** 0.3, 0.5, 1.5))
                    probs["Exitosa"] *= ajuste
                    res = elegir(rng, probs, 1)[0]
                    if liq and tipo in ("AcuerdoPago", "Visita") and res == "Exitosa":
                        res = "Rechazada"
                    acuerdo_id = 0
                    if tipo == "AcuerdoPago" and res == "Exitosa":
                        tramos_libres = [t for t in deuda[a] if libre(t) and t[1] < f]
                        if not tramos_libres:
                            res = "Pendiente"
                        else:
                            acuerdo_id = len(acuerdos) + 1
                            total = 0.0
                            for t in tramos_libres:
                                t[3] = interes(t, f)      # el interés se congela al firmar
                                t[4] = acuerdo_id
                                total += t[2] + t[3]
                            nc = int(np.clip(len(tramos_libres) + rng.integers(1, 4),
                                             ac["cuotas_min"], ac["cuotas_max"]))
                            valor = np.full(nc, math.floor(total / nc))
                            valor[-1] = round(total - valor[:-1].sum())
                            y, mo, d = ord_partes(np.array([f]))
                            venc = []
                            for c in range(1, nc + 1):
                                yy, mm = divmod(int(y[0]) * 12 + int(mo[0]) - 1 + c, 12)
                                dd = min(int(d[0]), 28)
                                venc.append(cal.siguiente_habil(date(yy, mm + 1, dd).toordinal()))
                            p_cumple = float(np.clip(ac["pct_cumplidos"] * (r_ref / A["riesgo"][a]) ** 0.25, 0.3, 0.95))
                            cumple = rng.random() < p_cumple
                            acuerdos[acuerdo_id] = {
                                "a": a, "fecha": f, "venc": venc, "valor": valor,
                                "pagado": np.zeros(nc), "fpago": np.zeros(nc, dtype=np.int64),
                                "corte_en": nc + 1 if cumple else int(rng.integers(1, nc + 1)),
                                "estado": "Vigente", "roto_en": 0,
                            }
                            acuerdo_activo[a] = acuerdo_id
                    if res == "Exitosa":
                        exito[a] = True
                    plantilla = cat.NOTAS[(tipo, res)]
                    nota = plantilla[rng.integers(len(plantilla))]
                    dia_prom = cal.siguiente_habil(int(f) + int(rng.integers(3, 15)))
                    yy, mm, dd = ord_partes(np.array([dia_prom]))
                    nota = nota.format(nombre=nombres[rng.integers(len(nombres))],
                                       cargo=cat.CARGOS[rng.integers(len(cat.CARGOS))],
                                       dia=f"{int(dd[0])} de {MESES[int(mm[0]) - 1].lower()}",
                                       monto=f"${sum(t[2] for t in deuda[a]):,.0f}".replace(",", "."))
                    gestiones.append((a, int(f), tipo, res, tr, gestores[rng.integers(len(gestores))],
                                      nota, acuerdo_id))

        # ---- 2. Cuotas de acuerdos que vencen en el mes -----------------------
        for aid in list(np.unique(acuerdo_activo[acuerdo_activo > 0])):
            g = acuerdos[aid]
            a = g["a"]
            for c, v in enumerate(g["venc"]):
                if not (ms <= v <= me) or g["fpago"][c] > 0:
                    continue
                if c + 1 < g["corte_en"]:
                    f = v - int(rng.integers(0, 6))
                    if rng.random() < ac["pct_cuota_tardia"]:
                        f = min(v + int(rng.integers(1, 11)), me)
                    if f > cal.corte:
                        break
                    pagar_fifo(a, g["valor"][c], f, "Abono acuerdo", lambda t, aid=aid: t[4] == aid, aid)
                    g["pagado"][c], g["fpago"][c] = g["valor"][c], f
                    if c == len(g["venc"]) - 1:
                        g["estado"] = "Cumplido"
                        acuerdo_activo[a] = 0
                        deuda[a] = [t for t in deuda[a] if t[4] != aid]
                else:  # se rompe el acuerdo: la deuda vuelve a causar intereses
                    g["estado"], g["roto_en"] = "Incumplido", c + 1
                    acuerdo_activo[a] = 0
                    for t in deuda[a]:
                        if t[4] == aid:
                            t[3], t[4] = -1.0, 0
                    break

        # ---- 3. Pagos extemporáneos espontáneos (ponerse al día) --------------
        for a in range(n):
            if not deuda[a] or acuerdo_activo[a] or A["fecha_liq"][a] <= ms:
                continue
            libres = [t for t in deuda[a] if libre(t)]
            if not libres:
                continue
            p_full = pg["cura_completa"] * float(np.clip(math.sqrt(r_ref / A["riesgo"][a]), 0.4, 2.0))
            p_full /= 1 + 0.2 * (len(libres) - 1)
            if exito[a]:
                p_full *= pg["cura_boost_gestion"]
            u = rng.random()
            f = int(hab[rng.integers(len(hab))])
            if u < p_full:
                pagar_fifo(a, 1e18, f, "PILA extemporánea", libre)
            elif u < p_full + pg["cura_parcial"]:
                viejo = min(libres, key=lambda t: t[1])
                pagar_fifo(a, viejo[2] + interes(viejo, f), f, "PILA extemporánea", libre)

        # ---- 4. Vence el periodo p = M-1: ¿cómo paga cada empresa? -----------
        p = M - 1
        activos = np.nonzero(E[:, p] > 0)[0]
        u1, u2, u3, u4 = (rng.random(len(activos)) for _ in range(4))
        for j, a in enumerate(activos):
            esperado = E[a, p]
            lim = int(limite[a, p])
            con_deuda = any(t[5] == 0 for t in deuda[a])
            r = r_mes[a, p]
            odds = r / (1 - r)
            if con_deuda:
                odds *= pg["odds_con_deuda"]
            if acuerdo_activo[a]:
                odds *= pg["odds_con_acuerdo"]
            p_mal = odds / (1 + odds)
            if A["fecha_liq"][a] <= lim:
                p_mal = pg["prob_liquidacion_impago"]
            habiles_ok = hab[hab <= lim]
            f_op = int(habiles_ok[rng.integers(len(habiles_ok))]) if len(habiles_ok) else lim
            if u1[j] < p_mal:
                if u2[j] < pg["pct_parcial"]:
                    fr = pg["fraccion_parcial"]
                    pagado = round(esperado * (fr[0] + u3[j] * (fr[1] - fr[0])))
                    registrar_pago(a, p, f_op, pagado, 0, "PILA oportuna")
                    deuda[a].append([p, lim, esperado - pagado, -1.0, 0, 0])
                    resultado[a, p] = PARCIAL
                else:
                    deuda[a].append([p, lim, esperado, -1.0, 0, 0])
                    resultado[a, p] = NO_PAGO
            elif u3[j] < pg["tardio_base"] + pg["tardio_pendiente"] * r:
                b = rng.choice(len(rango_dias["p"]), p=rango_dias["p"])
                lo, hi = rango_dias["rangos"][b]
                f = cal.siguiente_habil(lim + int(rng.integers(lo, hi + 1)))
                registrar_pago(a, p, f, esperado, esperado * dr * (f - lim), "PILA extemporánea")
                deuda[a].append([p, lim, esperado, -1.0, 0, f])   # programado: se paga en f
                resultado[a, p] = TARDIO
            else:
                registrar_pago(a, p, f_op, esperado, 0, "PILA oportuna")
                resultado[a, p] = OPORTUNO

        # ---- 5. Foto de cartera al cierre del mes -----------------------------
        dias = np.zeros(n, dtype=np.int64)
        cap = np.zeros(n)
        intr = np.zeros(n)
        nper = np.zeros(n, dtype=np.int64)
        for a in range(n):
            if not deuda[a]:
                continue
            vivos = [t for t in deuda[a] if (t[5] == 0 or t[5] > me) and t[1] <= me]
            if vivos:
                dias[a] = me - min(t[1] for t in vivos)
                cap[a] = sum(t[2] for t in vivos)
                intr[a] = sum(interes(t, me) for t in vivos)
                nper[a] = len({t[0] for t in vivos})
            deuda[a] = [t for t in deuda[a] if not (0 < t[5] <= me)]
        tramo = np.select([dias == 0, dias <= 30, dias <= 60, dias <= 90], [0, 1, 2, 3], 4)
        tramo_prev = tramo

        if publicar:
            visible = (A["aff"] <= me) & ((A["fin_emp"] > ms) | (cap > 0))
            idx = np.nonzero(visible)[0]
            for a in idx:
                mora_filas.append((a, M, nper[a], dias[a], cap[a], intr[a], tramo[a],
                                   acuerdo_activo[a] > 0))
            # Score por reglas (reglas-v1): días de mora, historial, sector, antigüedad.
            inc12 = ((resultado[idx, max(0, M - 12):M] == PARCIAL) |
                     (resultado[idx, max(0, M - 12):M] == NO_PAGO)).sum(axis=1)
            anios = (me - A["aff"][idx]) / 365.25
            z = (rr["intercepto"] + rr["w_dias_por_30"] * np.minimum(dias[idx], 180) / 30
                 + rr["w_incumplimientos_12m"] * inc12 + rr["w_sector_alto"] * sector_alto[idx]
                 + rr["w_nueva"] * (anios < 2) + rr["w_antigua"] * (anios > 10)
                 + rr["w_micro"] * es_micro[idx] + rng.normal(0, rr["ruido_sigma"], len(idx)))
            prob = 1 / (1 + np.exp(-z))
            for a, pr in zip(idx, prob):
                riesgo_filas.append((a, me, pr))

    return {"pagos": pagos, "gestiones": gestiones, "mora": mora_filas, "riesgo": riesgo_filas,
            "acuerdos": acuerdos, "resultado": resultado, "limite": limite, "gestores": gestores}


# =============================================================================
# Tablas de hechos
# =============================================================================
def tablas_hechos(cfg, A, E, N, S, cal):
    lbl = np.array(cal.label, dtype=object)
    rr = cfg["riesgo_reglas"]["umbrales"]

    # FactPagos
    pg = pd.DataFrame(S["pagos"], columns=["a", "p", "f", "cap", "int", "TipoPago", "Canal", "AcuerdoID"])
    pg = pg.sort_values(["f", "a", "p"]).reset_index(drop=True)
    fact_pagos = pd.DataFrame({
        "PagoID": np.arange(1, len(pg) + 1), "AportanteID": A["id"][pg["a"]],
        "PeriodoAplicado": lbl[pg["p"]], "FechaPago": ord_a_iso(pg["f"].to_numpy()),
        "ValorCapital": pg["cap"], "ValorInteres": pg["int"],
        "ValorPagado": pg["cap"] + pg["int"], "TipoPago": pg["TipoPago"], "Canal": pg["Canal"],
        "AcuerdoID": pg["AcuerdoID"].replace(0, pd.NA).astype("Int64"),
    })

    # FactAportes: lo esperado contra lo pagado al corte.
    a_idx, p_idx = np.nonzero(E[:, cal.out_first:cal.out_last + 1] > 0)
    p_idx = p_idx + cal.out_first
    clave = a_idx * cal.n_meses + p_idx
    g = pg.assign(k=pg["a"] * cal.n_meses + pg["p"]).groupby("k").agg(pagado=("cap", "sum"), ult=("f", "max"))
    # El redondeo de pagos parciales puede sumar 1-2 pesos de más: se acota a lo esperado.
    pagado = np.minimum(g["pagado"].reindex(clave).fillna(0).to_numpy(), E[a_idx, p_idx])
    ult = g["ult"].reindex(clave).fillna(0).astype(np.int64).to_numpy()
    esperado = E[a_idx, p_idx]
    lim = S["limite"][a_idx, p_idx]
    completo = pagado >= esperado - 1
    estado = np.select(
        [p_idx == cal.out_last, completo & (ult <= lim), completo, pagado > 0],
        ["Pendiente", "Oportuno", "Tardío", "Parcial"], "No pagado")
    retraso = np.where(estado == "Pendiente", 0,
                       np.maximum(np.where(completo, ult, cal.corte) - lim, 0))
    fact_aportes = pd.DataFrame({
        "AporteID": np.arange(1, len(a_idx) + 1), "Periodo": lbl[p_idx],
        "AportanteID": A["id"][a_idx], "NumeroCotizantes": N[a_idx, p_idx],
        "ValorEsperado": esperado.astype(np.int64), "ValorPagado": np.round(pagado).astype(np.int64),
        "FechaLimitePago": ord_a_iso(lim), "FechaPago": ord_a_iso(ult),
        "DiasRetraso": retraso, "EstadoPago": estado,
    }).sort_values(["Periodo", "AportanteID"]).reset_index(drop=True)
    fact_aportes["AporteID"] = np.arange(1, len(fact_aportes) + 1)

    # FactMora
    mo = pd.DataFrame(S["mora"], columns=["a", "M", "nper", "dias", "cap", "int", "tramo", "acuerdo"])
    fact_mora = pd.DataFrame({
        "MoraID": np.arange(1, len(mo) + 1), "Periodo": lbl[mo["M"]],
        "AportanteID": A["id"][mo["a"]], "PeriodosAdeudados": mo["nper"],
        "DiasMora": mo["dias"], "SaldoCapital": mo["cap"].round().astype(np.int64),
        "SaldoInteres": mo["int"].round().astype(np.int64),
        "SaldoTotal": (mo["cap"].round() + mo["int"].round()).astype(np.int64),
        "EstadoMora": np.array(TRAMOS, dtype=object)[mo["tramo"]], "TieneAcuerdo": mo["acuerdo"],
    })

    # FactCobranza
    ge = pd.DataFrame(S["gestiones"], columns=["a", "f", "TipoGestion", "ResultadoGestion",
                                               "TramoMoraAlGestionar", "Gestor", "NotaGestion", "AcuerdoID"])
    ge = ge.sort_values(["f", "a"]).reset_index(drop=True)
    fact_cobranza = pd.DataFrame({
        "GestionID": np.arange(1, len(ge) + 1), "AportanteID": A["id"][ge["a"]],
        "FechaGestion": ord_a_iso(ge["f"].to_numpy()), "TipoGestion": ge["TipoGestion"],
        "ResultadoGestion": ge["ResultadoGestion"], "TramoMoraAlGestionar": ge["TramoMoraAlGestionar"],
        "Gestor": ge["Gestor"], "NotaGestion": ge["NotaGestion"],
        "AcuerdoID": ge["AcuerdoID"].replace(0, pd.NA).astype("Int64"),
    })

    # FactAcuerdoPago: una fila por cuota, estado al corte.
    filas = []
    for aid, g in S["acuerdos"].items():
        nc = len(g["venc"])
        estado_ac = g["estado"]
        for c in range(nc):
            if g["fpago"][c] > 0:
                ec = "Pagada"
            elif estado_ac == "Incumplido":
                ec = "Vencida" if c + 1 == g["roto_en"] else "Anulada"
            elif g["venc"][c] <= cal.corte:
                ec = "Vencida"
            else:
                ec = "Pendiente"
            filas.append((aid, A["id"][g["a"]], g["fecha"], c + 1, g["venc"][c], int(g["valor"][c]),
                          int(g["pagado"][c]), g["fpago"][c], ec, estado_ac))
    acp = pd.DataFrame(filas, columns=["AcuerdoID", "AportanteID", "FechaAcuerdo", "NumeroCuota",
                                       "FechaVencimientoCuota", "ValorCuota", "ValorPagado",
                                       "FechaPagoCuota", "EstadoCuota", "EstadoAcuerdo"])
    for col in ("FechaAcuerdo", "FechaVencimientoCuota", "FechaPagoCuota"):
        acp[col] = ord_a_iso(acp[col].to_numpy())

    # FactRiesgo (score por reglas; el modelo de ML escribirá ml-v1 después)
    ri = pd.DataFrame(S["riesgo"], columns=["a", "f", "prob"])
    nivel = np.select([ri["prob"] < rr["Medio"], ri["prob"] < rr["Alto"], ri["prob"] < rr["Critico"]],
                      ["Bajo", "Medio", "Alto"], "Critico")
    fact_riesgo = pd.DataFrame({
        "AportanteID": A["id"][ri["a"]], "FechaEvaluacion": ord_a_iso(ri["f"].to_numpy()),
        "ProbabilidadIncumplimiento": ri["prob"].round(4), "NivelRiesgo": nivel,
        "ScoreVersion": "reglas-v1",
    })
    return fact_aportes, fact_pagos, fact_mora, fact_cobranza, acp, fact_riesgo


# =============================================================================
# Errores intencionales (se aplican solo a la copia que se escribe)
# =============================================================================
def ensuciar(cfg, rng, tablas, A):
    er = cfg["errores"]
    dim_ap = tablas["DimAportante"]
    m = rng.random(len(dim_ap)) < er["dv_invalido"]
    dim_ap.loc[m, "DigitoVerificacion"] = (dim_ap.loc[m, "DigitoVerificacion"] + rng.integers(1, 10, m.sum())) % 10
    m = rng.random(len(dim_ap)) < er["razon_social_sucia"]
    dim_ap.loc[m, "RazonSocial"] = ["  " + s.upper() + " " if rng.random() < 0.5 else s.lower() + "   "
                                    for s in dim_ap.loc[m, "RazonSocial"]]

    dim_cot = tablas["DimCotizante"]
    m = rng.random(len(dim_cot)) < er["cotizante_fk_inexistente"]
    dim_cot.loc[m, "AportanteID"] = [f"APT-9{rng.integers(0, 99999):05d}" for _ in range(m.sum())]

    pagos = tablas["FactPagos"]
    m = rng.random(len(pagos)) < er["pagos_fecha_imposible"]
    per = pd.to_datetime(pagos.loc[m, "PeriodoAplicado"] + "-01")
    pagos.loc[m, "FechaPago"] = (per - pd.Timedelta(days=60)).dt.strftime("%Y-%m-%d").to_numpy()
    dup = pagos.sample(frac=er["pagos_duplicados"], random_state=int(rng.integers(1 << 31)))
    tablas["FactPagos"] = pd.concat([pagos, dup]).sort_values("PagoID", kind="stable").reset_index(drop=True)

    ap = tablas["FactAportes"]
    m = (rng.random(len(ap)) < er["aportes_monto_negativo"]) & (ap["ValorPagado"] > 0)
    ap.loc[m, "ValorPagado"] = -ap.loc[m, "ValorPagado"]
    return tablas


# =============================================================================
# Escritura y resumen
# =============================================================================
CLAVE_PARTICION = {
    "FactAporteDetalle": "Periodo", "FactAportes": "Periodo", "FactMora": "Periodo",
    "FactPagos": "FechaPago", "FactCobranza": "FechaGestion",
    "FactAcuerdoPago": "FechaAcuerdo", "FactRiesgo": "FechaEvaluacion",
}


def escribir(tablas: dict, salida: Path, particionar: bool):
    salida.mkdir(parents=True, exist_ok=True)
    tamanos = {}
    for nombre, df in tablas.items():
        if particionar and nombre in CLAVE_PARTICION:
            carpeta = salida / nombre
            carpeta.mkdir(exist_ok=True)
            mes = df[CLAVE_PARTICION[nombre]].astype(str).str[:7]
            total = 0
            for m, parte in df.groupby(mes, sort=True):
                ruta = carpeta / f"{nombre}_{m}.csv"
                parte.to_csv(ruta, index=False)
                total += ruta.stat().st_size
            tamanos[nombre] = total
        else:
            ruta = salida / f"{nombre}.csv"
            df.to_csv(ruta, index=False)
            tamanos[nombre] = ruta.stat().st_size
    return tamanos


def resumen(tablas, tamanos, salida, segundos):
    hechos = [k for k in tablas if k.startswith("Fact")]
    filas = {k: len(v) for k, v in tablas.items()}
    mora = tablas["FactMora"]
    ultimo = mora["Periodo"].max()
    pct_alguna = mora.loc[mora["DiasMora"] > 0, "AportanteID"].nunique() / mora["AportanteID"].nunique()
    m_ult = mora[mora["Periodo"] == ultimo]
    pct_ult = (m_ult["DiasMora"] > 0).mean()
    ri = tablas["FactRiesgo"]
    dist = (ri[ri["FechaEvaluacion"] == ri["FechaEvaluacion"].max()]["NivelRiesgo"]
            .value_counts(normalize=True).reindex(["Bajo", "Medio", "Alto", "Critico"]).fillna(0))

    print("\n" + "=" * 64)
    print(f"{'Tabla':<22}{'Filas':>14}{'Tamaño (MB)':>16}")
    print("-" * 64)
    for k in tablas:
        print(f"{k:<22}{filas[k]:>14,}{tamanos[k] / 1e6:>16,.1f}")
    print("-" * 64)
    print(f"{'Total hechos':<22}{sum(filas[k] for k in hechos):>14,}"
          f"{sum(tamanos[k] for k in hechos) / 1e6:>16,.1f}")
    print(f"{'Total':<22}{sum(filas.values()):>14,}{sum(tamanos.values()) / 1e6:>16,.1f}")
    print("=" * 64)
    print(f"Aportantes con mora alguna vez (2023-2025): {pct_alguna:.1%}")
    print(f"Aportantes en mora al cierre de {ultimo}:  {pct_ult:.1%}")
    print("Distribución de riesgo al último corte:  " +
          " · ".join(f"{k} {v:.1%}" for k, v in dist.items()))
    print(f"Archivos en: {salida.resolve()}   ({segundos:.0f} s)")

    info = {"filas": filas, "bytes": tamanos, "pct_aportantes_con_mora": round(pct_alguna, 4),
            "pct_en_mora_ultimo_corte": round(float(pct_ult), 4),
            "distribucion_riesgo": {k: round(float(v), 4) for k, v in dist.items()}}
    (salida / "_resumen.json").write_text(json.dumps(info, indent=2, ensure_ascii=False))
    return info


# =============================================================================
# Orquestación
# =============================================================================
def generar(config: str | Path = "config/parametros.yaml", salida: str | Path | None = None,
            escala: float | None = None, particionar: bool = False, sin_errores: bool = False):
    t0 = time.time()
    cfg = yaml.safe_load(Path(config).read_text(encoding="utf-8"))
    if escala is not None:
        cfg["escala"] = escala
    salida = Path(salida or "data/raw")
    rng = np.random.default_rng(cfg["semilla"])
    Faker.seed(cfg["semilla"])
    fake = Faker("es_CO")

    print("1/6 Calendario y geografía…")
    cal = Calendario(cfg)
    geo, pesos_geo = generar_geografia()
    print("2/6 Aportantes…")
    A = generar_aportantes(cfg, rng, fake, cal, pesos_geo, len(geo))
    print("3/6 Cotizantes…")
    C = generar_cotizantes(cfg, rng, A, cal)
    print("4/6 Detalle de aportes…")
    det, E, N = generar_detalle(cfg, C, A, cal)
    print("5/6 Simulación mensual de pagos, mora y cobranza…")
    S = simular(cfg, rng, fake, A, E, cal)
    fact_aportes, fact_pagos, fact_mora, fact_cob, fact_acu, fact_riesgo = tablas_hechos(cfg, A, E, N, S, cal)

    tablas = {
        "DimAportante": tabla_dim_aportante(A, cal), "DimCotizante": tabla_dim_cotizante(C, A, cal),
        "DimTiempo": tabla_dim_tiempo(cal), "DimGeografia": geo,
        "FactAporteDetalle": det, "FactAportes": fact_aportes, "FactPagos": fact_pagos,
        "FactMora": fact_mora, "FactCobranza": fact_cob, "FactAcuerdoPago": fact_acu,
        "FactRiesgo": fact_riesgo,
    }
    if not sin_errores:
        tablas = ensuciar(cfg, rng, tablas, A)
    print("6/6 Escribiendo CSV…")
    tamanos = escribir(tablas, salida, particionar)
    return resumen(tablas, tamanos, salida, time.time() - t0)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Genera el dataset sintético de recaudo y cartera.")
    ap.add_argument("--config", default="config/parametros.yaml")
    ap.add_argument("--salida", default="data/raw")
    ap.add_argument("--escala", type=float, default=None, help="Sobrescribe 'escala' del YAML")
    ap.add_argument("--particionar", action="store_true", help="Un archivo por mes en cada tabla de hechos")
    ap.add_argument("--sin-errores", action="store_true", help="No inyecta errores de calidad")
    args = ap.parse_args(argv)
    generar(args.config, args.salida, args.escala, args.particionar, args.sin_errores)


if __name__ == "__main__":
    main()
