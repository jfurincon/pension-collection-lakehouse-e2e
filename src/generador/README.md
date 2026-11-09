# Generador de datos sintéticos

Simula 36 meses (2023-01 a 2025-12) de recaudo de aportes a pensión de 10.000 empresas y unos 100.000 cotizantes activos por mes. Los datos salen como CSV relacionados entre sí, listos para la capa Bronze.

## Uso

```bash
pip install -r requirements.txt

# Dataset completo (~5,2 M filas de hechos, ~365 MB, ~1 minuto)
python src/generador/generar.py --salida data/raw

# 10% del tamaño para iterar rápido (~6 segundos)
python src/generador/generar.py --escala 0.1 --salida data/raw

# Un archivo por mes en cada tabla de hechos (para simular cargas incrementales con Auto Loader)
python src/generador/generar.py --particionar --salida data/raw

# Sin los errores de calidad inyectados
python src/generador/generar.py --sin-errores

# Comprobar las 12 metas de correlación (sale con código 1 si alguna falla)
python src/generador/validar.py --datos data/raw
```

En un notebook de Databricks o Fabric:

```python
import sys; sys.path.append("src/generador")
from generar import generar
generar("config/parametros.yaml", "/Volumes/recaudo_dev/raw/landing")   # Databricks
generar("config/parametros.yaml", "/lakehouse/default/Files/raw")       # Fabric
```

## Cómo funciona

1. **Aportantes.** Cada empresa recibe sector, tamaño, ciudad, fecha de afiliación y un *riesgo latente* oculto = riesgo del sector × factor de tamaño × ruido lognormal. La antigüedad se aplica mes a mes, así que una empresa joven se vuelve menos riesgosa con el tiempo. Un 6% cierra, un 1,5% entra en liquidación (las riesgosas tienen más probabilidad) y un 3% se traslada de ciudad.
2. **Cotizantes.** Cada empresa tiene un número de puestos según su tamaño. Un 25% de los puestos tiene un retiro y un reemplazo dentro de la ventana. Los salarios son múltiplos del SMMLV: ~30% gana el mínimo exacto y el resto sigue una franja uniforme y una cola lognormal.
3. **Detalle de aportes.** Por cotizante y mes se calcula IBC (proporcional a los días cotizados), cotización del 16% y Fondo de Solidaridad Pensional. La suma por empresa es el valor esperado.
4. **Simulación mensual.** Cuando vence cada periodo (k-ésimo día hábil del mes siguiente, según el NIT), la empresa paga a tiempo, tarde, una parte o nada. La probabilidad depende del riesgo latente, de si ya debe (la mora es persistente) y de si tiene un acuerdo vigente. Cada mes:
    - las empresas en mora reciben gestiones de cobranza (más mora, más gestiones);
    - una gestión de tipo AcuerdoPago exitosa congela los intereses y reparte la deuda en cuotas;
    - las empresas sin acuerdo pueden ponerse al día por su cuenta;
    - al cierre se toma la foto de cartera (días de mora, capital, interés) y el score de riesgo por reglas.
5. **Calentamiento.** La simulación arranca 12 meses antes (2022) para que la cartera de enero de 2023 no empiece en cero. Por eso `FactPagos` incluye pagos de periodos de 2022: es cartera heredada y no tiene fila en `FactAportes`.

Todos los parámetros están en `config/parametros.yaml`, con la misma semilla el resultado es idéntico. El contrato de columnas está en la pestaña *Diccionario de datos* del plan del proyecto.

--------Test---------
Prueba CI **Hola Mundo**
