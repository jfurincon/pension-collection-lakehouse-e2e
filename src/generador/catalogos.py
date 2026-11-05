"""Catálogos fijos del generador: geografía, CIIU, nombres y plantillas de texto.

Son listas de referencia, no parámetros de comportamiento (esos están en
config/parametros.yaml). Los códigos DANE y CIIU son reales pero conviene
verificarlos antes de publicar.
"""

# Regional del fondo por departamento.
REGIONAL_POR_DEPTO = {
    "Bogotá D.C.": "Bogotá", "Cundinamarca": "Bogotá",
    "Antioquia": "Antioquia y Eje Cafetero", "Caldas": "Antioquia y Eje Cafetero",
    "Risaralda": "Antioquia y Eje Cafetero", "Quindío": "Antioquia y Eje Cafetero",
    "Chocó": "Antioquia y Eje Cafetero",
    "Atlántico": "Caribe", "Bolívar": "Caribe", "Magdalena": "Caribe", "Cesar": "Caribe",
    "Córdoba": "Caribe", "Sucre": "Caribe", "La Guajira": "Caribe",
    "San Andrés y Providencia": "Caribe",
    "Valle del Cauca": "Pacífico", "Cauca": "Pacífico", "Nariño": "Pacífico",
    "Putumayo": "Pacífico",
}  # el resto -> "Centro Oriente"

# (Ciudad, Departamento, Código DANE, población aprox. en miles)
CIUDADES = [
    ("Bogotá", "Bogotá D.C.", "11001", 7900),
    ("Medellín", "Antioquia", "05001", 2600), ("Bello", "Antioquia", "05088", 560),
    ("Itagüí", "Antioquia", "05360", 290), ("Envigado", "Antioquia", "05266", 240),
    ("Rionegro", "Antioquia", "05615", 140), ("Sabaneta", "Antioquia", "05631", 90),
    ("Apartadó", "Antioquia", "05045", 200), ("Turbo", "Antioquia", "05837", 130),
    ("Cali", "Valle del Cauca", "76001", 2280), ("Palmira", "Valle del Cauca", "76520", 360),
    ("Buenaventura", "Valle del Cauca", "76109", 320), ("Tuluá", "Valle del Cauca", "76834", 220),
    ("Jamundí", "Valle del Cauca", "76364", 190), ("Cartago", "Valle del Cauca", "76147", 140),
    ("Buga", "Valle del Cauca", "76111", 130), ("Yumbo", "Valle del Cauca", "76892", 110),
    ("Barranquilla", "Atlántico", "08001", 1300), ("Soledad", "Atlántico", "08758", 680),
    ("Malambo", "Atlántico", "08433", 130),
    ("Cartagena", "Bolívar", "13001", 1060), ("Magangué", "Bolívar", "13430", 140),
    ("Cúcuta", "Norte de Santander", "54001", 790), ("Ocaña", "Norte de Santander", "54498", 100),
    ("Soacha", "Cundinamarca", "25754", 800), ("Fusagasugá", "Cundinamarca", "25290", 160),
    ("Facatativá", "Cundinamarca", "25269", 150), ("Chía", "Cundinamarca", "25175", 150),
    ("Zipaquirá", "Cundinamarca", "25899", 140), ("Mosquera", "Cundinamarca", "25473", 140),
    ("Girardot", "Cundinamarca", "25307", 110), ("Madrid", "Cundinamarca", "25430", 120),
    ("Funza", "Cundinamarca", "25286", 100), ("Cajicá", "Cundinamarca", "25126", 90),
    ("Bucaramanga", "Santander", "68001", 620), ("Floridablanca", "Santander", "68276", 280),
    ("Girón", "Santander", "68307", 200), ("Piedecuesta", "Santander", "68547", 170),
    ("Barrancabermeja", "Santander", "68081", 220),
    ("Villavicencio", "Meta", "50001", 560),
    ("Ibagué", "Tolima", "73001", 550),
    ("Santa Marta", "Magdalena", "47001", 540), ("Ciénaga", "Magdalena", "47189", 110),
    ("Valledupar", "Cesar", "20001", 530),
    ("Montería", "Córdoba", "23001", 510),
    ("Pereira", "Risaralda", "66001", 480), ("Dosquebradas", "Risaralda", "66170", 230),
    ("Manizales", "Caldas", "17001", 450),
    ("Pasto", "Nariño", "52001", 400),
    ("Neiva", "Huila", "41001", 370),
    ("Armenia", "Quindío", "63001", 310),
    ("Popayán", "Cauca", "19001", 330),
    ("Sincelejo", "Sucre", "70001", 300),
    ("Riohacha", "La Guajira", "44001", 290), ("Maicao", "La Guajira", "44430", 170),
    ("Tunja", "Boyacá", "15001", 200), ("Sogamoso", "Boyacá", "15759", 120),
    ("Duitama", "Boyacá", "15238", 120),
    ("Florencia", "Caquetá", "18001", 180),
    ("Yopal", "Casanare", "85001", 180),
    ("Quibdó", "Chocó", "27001", 130),
    ("Arauca", "Arauca", "81001", 95),
    ("Mocoa", "Putumayo", "86001", 60),
    ("San Andrés", "San Andrés y Providencia", "88001", 60),
    ("Leticia", "Amazonas", "91001", 50),
    ("San José del Guaviare", "Guaviare", "95001", 70),
    ("Mitú", "Vaupés", "97001", 30),
    ("Puerto Carreño", "Vichada", "99001", 20),
    ("Inírida", "Guainía", "94001", 30),
]

# Bogotá concentra más empleadores formales que población: se fuerza ~35%.
PCT_APORTANTES_BOGOTA = 0.35

# Clases CIIU (4 dígitos) y prefijos de razón social por sector.
CIIU = {
    "Construcción": ["4111", "4112", "4210", "4290", "4330"],
    "Transporte": ["4921", "4923", "5210", "5229"],
    "Comercio": ["4711", "4719", "4530", "4659", "4773"],
    "Industria": ["1011", "1410", "2229", "2511", "3110"],
    "Servicios": ["6910", "7020", "7310", "8121", "8299"],
    "Agro": ["0111", "0123", "0141", "0150"],
    "Salud": ["8610", "8621", "8692"],
    "Educación": ["8511", "8523", "8544"],
    "Tecnología": ["6201", "6202", "6311"],
}

PREFIJOS = {
    "Construcción": ["Constructora", "Obras Civiles", "Ingeniería y Construcciones", "Edificaciones"],
    "Transporte": ["Transportes", "Logística", "Cooperativa de Transportadores", "Carga y Encomiendas"],
    "Comercio": ["Comercializadora", "Distribuidora", "Almacenes", "Supermercado"],
    "Industria": ["Industrias", "Confecciones", "Manufacturas", "Plásticos"],
    "Servicios": ["Inversiones", "Consultores", "Servicios Integrales", "Asesorías"],
    "Agro": ["Agropecuaria", "Hacienda", "Cultivos", "Avícola"],
    "Salud": ["Clínica", "IPS", "Centro Médico", "Laboratorio Clínico"],
    "Educación": ["Colegio", "Instituto", "Centro de Formación", "Gimnasio"],
    "Tecnología": ["Soluciones Tecnológicas", "Software", "Sistemas", "Datos y Analítica"],
}

SUFIJOS_SOCIEDAD = ["S.A.S.", "S.A.S.", "S.A.S.", "S.A.", "Ltda.", "y Cía. S.A.S."]

# Plantillas de nota de gestión. {nombre}, {cargo}, {dia} y {monto} se rellenan.
CARGOS = ["contador", "gerente", "auxiliar contable", "jefe de nómina", "representante legal",
          "asistente administrativa", "revisor fiscal"]

NOTAS = {
    ("Llamada", "Exitosa"): [
        "Se habló con {nombre}, {cargo}. Confirma que pagan el {dia} por PILA.",
        "{nombre} ({cargo}) reconoce la deuda y promete ponerse al día este mes.",
        "Contacto efectivo con {nombre}. Indica que el pago se retrasó por flujo de caja; paga el {dia}.",
    ],
    ("Llamada", "Sin Contacto"): [
        "Número no contesta. Se deja mensaje de voz.",
        "Teléfono fuera de servicio. Se solicita actualizar datos de contacto.",
        "Contesta recepción; {nombre} no se encuentra. Se vuelve a llamar.",
    ],
    ("Llamada", "Rechazada"): [
        "{nombre} ({cargo}) indica que no reconoce el saldo y pide soporte detallado.",
        "El aportante manifiesta que no tiene recursos para pagar por ahora.",
    ],
    ("Llamada", "Pendiente"): [
        "{nombre} solicita estado de cuenta para validar con contabilidad.",
        "Se envía estado de cuenta; queda pendiente confirmación de fecha de pago.",
    ],
    ("Correo", "Exitosa"): [
        "Responden el correo confirmando pago para el {dia}.",
        "{nombre} responde que ya programó el pago de los periodos pendientes.",
    ],
    ("Correo", "Sin Contacto"): [
        "Correo rebotado; dirección inválida.",
        "Se envía requerimiento por correo; sin respuesta.",
    ],
    ("Correo", "Rechazada"): [
        "Responden que la obligación corresponde a trabajadores ya retirados.",
    ],
    ("Correo", "Pendiente"): [
        "Solicitan liquidación detallada por cotizante antes de pagar.",
        "Acusan recibo del requerimiento; quedan de revisar.",
    ],
    ("Visita", "Exitosa"): [
        "Visita a la sede. {nombre} ({cargo}) se compromete a pagar el {dia}.",
        "Visita efectiva: se revisa la deuda con {nombre} y acepta el saldo.",
    ],
    ("Visita", "Sin Contacto"): [
        "Visita fallida: la empresa no funciona en la dirección registrada.",
        "En la visita no se encontró al responsable de nómina.",
    ],
    ("Visita", "Rechazada"): [
        "En la visita el representante legal se niega a firmar compromiso de pago.",
    ],
    ("Visita", "Pendiente"): [
        "Se entrega requerimiento físico; {nombre} revisará con la gerencia.",
    ],
    ("AcuerdoPago", "Exitosa"): [
        "Se firma acuerdo de pago con {nombre} ({cargo}) por {monto} en cuotas mensuales.",
        "Acuerdo de pago aprobado; primera cuota el próximo mes.",
    ],
    ("AcuerdoPago", "Sin Contacto"): [
        "No fue posible contactar al aportante para formalizar el acuerdo.",
    ],
    ("AcuerdoPago", "Rechazada"): [
        "El aportante no acepta las condiciones del acuerdo de pago.",
        "Acuerdo rechazado: solicitan más cuotas de las permitidas.",
    ],
    ("AcuerdoPago", "Pendiente"): [
        "Propuesta de acuerdo enviada a {nombre}; pendiente de firma.",
    ],
}
