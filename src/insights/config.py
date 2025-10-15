# src/insights/config.py
from dataclasses import dataclass

# Polaridad de métricas (True = más alto es mejor; False = más alto es peor)
METRIC_POLARITY = {
    "Lead Penetration": True,
    "Perfect Orders": True,
    "Gross Profit UE": True,
    "Orders": True,  # cambia si tu dataset usa otro nombre mapeado desde el parser
}

# Umbrales globales
ANOMALY_WOW_THRESHOLD = 0.10   # ±10% WoW
TREND_MIN_RUN = 3              # >=3 caídas/altas consecutivas
TREND_MIN_R2 = 0.20            # pendiente con R² mínimo
BENCHMARK_Z_ABS = 1.5          # |z-score| >= 1.5
CORR_MIN_ABS = 0.5             # |ρ| >= 0.5
MIN_POINTS_TIME = 6            # mínimo puntos para series (L5..L0 = 6)
TOP_N = 10                     # nº por categoría

# Recomendaciones base (plantillas)
RECO_TEMPLATES = {
    "Perfect Orders_low": "Revisar fulfillment y control de calidad; auditar tiempos de preparación/entrega.",
    "Lead Penetration_low": "Refuerzo de adquisición/activación y campañas de prospección.",
    "Gross Profit UE_low": "Optimizar pricing/descuentos y mezcla de categorías; evaluar costos logísticos.",
    "Orders_growth_high_PO_low": "Priorizar estabilización operativa: mejorar PO para sostener el crecimiento de órdenes.",
    "Benchmark_negative": "Comparar playbooks con zonas pares de alto desempeño y replicar prácticas efectivas.",
    "Correlation_LP_PO": "Iniciativas conjuntas para elevar LP y PO (onboarding + calidad de servicio).",
}

