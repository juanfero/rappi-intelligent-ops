# src/bot/memory.py

from __future__ import annotations
from typing import Any, Dict, Optional


class Memory:
    """
    Memoria conversacional para filtros geográficos.

    Reglas principales:
      - Persistir `country` SOLO si fue mencionado explícitamente en la consulta
        (el parser debe establecer spec.context['explicit_country'] = True).
        Si no hay contexto, se aplica una heurística conservadora:
          * si spec.filters.country es None -> limpiar country en memoria
          * si spec.filters.country tiene valor -> tratar como explícito y persistir
      - Limpiar siempre `country` cuando la consulta agrupa por país
        (group_by contiene 'country'), para evitar el efecto de "país pegado".
      - city, zone y zone_type se persisten si llegan en el spec.
    """

    def __init__(self) -> None:
        self.state: Dict[str, Optional[str]] = {
            "country": None,
            "city": None,
            "zone": None,
            "zone_type": None,
        }

    def update_from_spec(self, spec: Any) -> None:
        """
        Actualiza la memoria a partir de un spec analítico.
        Este método es tolerante a ausencia de atributos (parser en evolución).
        """
        filters = getattr(spec, "filters", None)
        group_by = getattr(spec, "group_by", None) or []
        context = getattr(spec, "context", {}) or {}

        # Normaliza group_by para comparaciones robustas
        group_by_norm = [
            (g.lower() if isinstance(g, str) else g) for g in group_by
        ]

        # --------- COUNTRY (reglas de higiene) ---------
        explicit_country = bool(context.get("explicit_country", False))
        incoming_country = getattr(filters, "country", None) if filters else None

        if "country" in group_by_norm:
            # Si agrupas por país, NUNCA arrastres filtro de país.
            self.state["country"] = None
        else:
            if context:
                # Si el parser proporcionó contexto, confíe en él.
                if explicit_country and incoming_country:
                    self.state["country"] = incoming_country
                else:
                    # No fue explícito o no vino país -> limpiar para evitar "pegues".
                    self.state["country"] = None
            else:
                # Sin contexto: heurística conservadora.
                if incoming_country:
                    self.state["country"] = incoming_country
                else:
                    self.state["country"] = None

        # --------- CITY ---------
        incoming_city = getattr(filters, "city", None) if filters else None
        if incoming_city:
            self.state["city"] = incoming_city

        # --------- ZONE ---------
        incoming_zone = getattr(filters, "zone", None) if filters else None
        if incoming_zone:
            self.state["zone"] = incoming_zone

        # --------- ZONE TYPE ---------
        incoming_zone_type = getattr(filters, "zone_type", None) if filters else None
        if incoming_zone_type:
            self.state["zone_type"] = incoming_zone_type

    def reset(self) -> None:
        """Limpia todos los filtros recordados."""
        for k in list(self.state.keys()):
            self.state[k] = None

    def get(self) -> Dict[str, Optional[str]]:
        """Devuelve una copia del estado para evitar mutaciones externas."""
        return dict(self.state)
