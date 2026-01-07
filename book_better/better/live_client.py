from __future__ import annotations

import datetime
import functools
import logging
from collections.abc import Callable
from typing import Concatenate, Optional
from dataclasses import dataclass
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from requests_toolbelt.sessions import BaseUrlSession  # type: ignore
from urllib3.util import Retry
from requests.exceptions import JSONDecodeError

from book_better.enums import BetterActivity, BetterVenue
from book_better.logging import log_method_inputs_and_outputs
from book_better.models import (
    ActivityCart,
    ActivitySlot,
    ActivityTime,
)

type _LiveBetterClientInstanceMethod[**P, R] = Callable[
    Concatenate[LiveBetterClient, P], R
]


def _requires_authentication[**P, R](
    func: _LiveBetterClientInstanceMethod[P, R],
) -> _LiveBetterClientInstanceMethod[P, R]:
    @functools.wraps(func)
    def wrapper(self: LiveBetterClient, *args: P.args, **kwargs: P.kwargs) -> R:
        if not self.authenticated:
            logging.info(
                "requires_authentication: client is not authenticated, will authenticate"
            )
            self.authenticate()
        return func(self, *args, **kwargs)

    return wrapper

@dataclass
class CartSummary:
    id: int
    source: str
    total: int      
    item_hash: str
    general_credit_available: int
    general_credit_max_applicable: int

class LiveBetterClient:
    HEADERS = {
        "Origin": "https://bookings.better.org.uk",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0",
    }

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_url = "https://better-admin.org.uk"

        self.session: requests.Session = BaseUrlSession(
            base_url="https://better-admin.org.uk/api/"
        )
        self.session.headers.update(self.HEADERS)
        self.session.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=3,
                    backoff_factor=2,
                    status_forcelist=[429, 500, 502, 503, 504],
                )
            ),
        )

    @property
    @log_method_inputs_and_outputs
    def authenticated(self) -> bool:
        return bool(self.session.headers.get("Authorization"))

    @functools.cached_property
    @_requires_authentication
    @log_method_inputs_and_outputs
    def membership_user_id(self) -> Optional[int]:
        response = self.session.get("auth/user")
        response.raise_for_status()

        data = response.json().get("data", {}) or {}
        membership_user = data.get("membership_user")

        if not membership_user:
            logging.info(
                "No membership_user asociado a esta cuenta; usando membership_user_id=None."
            )
            return None

        return membership_user["id"]

    @log_method_inputs_and_outputs
    def authenticate(self) -> None:
        auth_response = self.session.post(
            "auth/customer/login",
            json=dict(username=self.username, password=self.password),
        )
        auth_response.raise_for_status()

        token: str = auth_response.json()["token"]
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    @_requires_authentication
    @log_method_inputs_and_outputs
    def get_available_slots_for(
        self,
        venue: BetterVenue,
        activity: BetterActivity,
        activity_date: datetime.date,
        start_time: datetime.time,
        end_time: datetime.time,
    ) -> list[ActivitySlot]:
        """
        Devuelve los *slots* (cancha concreta) para una franja horaria y fecha.

        - Pide los slots de Better.
        - Filtra solo los slots con plazas libres (spaces > 0) y status 'BOOK'.
        - Si todos están FULL o no bookeables, devuelve [] y muestra un mensaje corto.
        """
        url = f"{self.base_url}/api/activities/venue/{venue.value}/activity/{activity.value}/slots"
        params = dict(
            date=activity_date.isoformat(),
            start_time=start_time.strftime("%H:%M"),
            end_time=end_time.strftime("%H:%M"),
        )

        response = self.session.get(url, params=params)
        response.raise_for_status()

        data = response.json().get("data", [])

        # Filtrar solo slots con plazas libres y que se puedan reservar (status 'BOOK')
        filtered: list[dict] = []
        for s in data:
            spaces = s.get("spaces", 0)
            action = s.get("action_to_show") or {}
            status = action.get("status")
            if spaces > 0 and status == "BOOK":
                filtered.append(s)

        if not filtered:
            logging.info(
                "No hay canchas libres el %s de %s–%s.",
                activity_date.isoformat(),
                start_time.strftime("%H:%M"),
                end_time.strftime("%H:%M"),
            )
            return []

        slots: list[ActivitySlot] = []
        for s in filtered:
            slots.append(
                ActivitySlot(
                    id=s["id"],
                    location_id=s["location"]["id"],
                    pricing_option_id=s["pricing_option_id"],
                    restriction_ids=s.get("restriction_ids", []),
                    name=s["location"]["slug"],
                    cart_type=s["cart_type"],
                )
            )

        return slots

    
    
    def get_cart_summary(self) -> CartSummary:
        """
        Lee el carrito de actividades actual y devuelve total, itemHash y créditos disponibles.
        Usa GET /api/activities/cart
        """
        # OJO: aquí NO ponemos "api/activities/cart", solo "activities/cart"
        url = "activities/cart"
        response = self.session.get(url)
        response.raise_for_status()
        data = response.json()["data"]

        credits_general = data.get("credits", {}).get("general", {}) or {}

        return CartSummary(
            id=data["id"],
            source=data["source"],
            total=data["total"],
            item_hash=data["itemHash"],
            general_credit_available=credits_general.get("total_available", 0),
            general_credit_max_applicable=credits_general.get("max_applicable", 0),
        )

    @_requires_authentication
    def get_cart_raw(self) -> dict:
        """
        Devuelve el JSON completo del carrito (GET /api/activities/cart).
        """
        resp = self.session.get("activities/cart")
        resp.raise_for_status()
        return resp.json().get("data", {}) or {}

    @_requires_authentication
    def cart_contains_slot_id(self, slot_id: int) -> bool:
        """
        True si el carrito ya contiene un item cuyo 'id' coincida con el slot.id
        (evita agregar el mismo slot 2 veces).
        """
        data = self.get_cart_raw()
        items = data.get("items") or data.get("cart_items") or data.get("lines") or []
        for it in items:
            try:
                # En Better, normalmente el item lleva el id del slot (o algo equivalente).
                if int(it.get("id")) == int(slot_id):
                    return True
            except Exception:
                continue
        return False


    def apply_credit(self, amount: int, cart_source: str) -> None:
        payload = {
            "credits_to_reserve": [
                {
                    "amount": amount,
                    "type": "general",
                }
            ],
            "cart_source": cart_source,
            "selected_user_id": None,
        }

        response = self.session.post("credits/apply", json=payload)

        if response.status_code != 200:
            logging.error("Error al aplicar crédito (status %s).", response.status_code)
        else:
            logging.info("Crédito aplicado correctamente.")

        response.raise_for_status()



    def checkout_with_credit(self, cart_id: int, item_hash: str, amount: int, source: str) -> dict:
        """
        Completa el checkout pagando con crédito general.
        Replica el payload de /api/checkout/complete que vimos en el navegador.
        """

        payload = {
            "completed_waivers": [],
            "payments": [
                {
                    "tender_type": "credit",
                    "amount": amount,
                    "info": {},
                }
            ],
            "item_hash": item_hash,
            "selected_user_id": None,
            "source": source,   # normalmente "activity-booking"
            "terms": [1],
        }

        # IMPORTANTE: ruta relativa, nada de self.base_url ni '/api' aquí
        response = self.session.post("checkout/complete", json=payload)

        if response.status_code != 200:
            logging.error("Error al completar el pago con crédito (status %s).", response.status_code)
        else:
            logging.info("Reserva pagada correctamente con crédito.")

        response.raise_for_status()
        return response.json()


    @_requires_authentication
    @log_method_inputs_and_outputs
    def get_available_times_for(
        self, venue: BetterVenue, activity: BetterActivity, activity_date: datetime.date
    ) -> list[ActivityTime]:
        response = self.session.get(
            f"activities/venue/{venue.value}/activity/{activity.value}/times",
            params={"date": activity_date.strftime("%Y-%m-%d")},
        )
        response.raise_for_status()

        try:
            data = response.json()
        except JSONDecodeError:
            # Cuando la semana aún no está abierta, Better devuelve HTML (redirige a /auth),
            # no JSON. En ese caso lo interpretamos como "no hay horas disponibles todavía".
            logging.info(
                "Todavía no abre la ventana de reservas para %s.",
                activity_date,
            )
            return []

        times_data = data.get("data", [])

        return [
            ActivityTime(
                start=datetime.datetime.strptime(
                    time_["starts_at"]["format_24_hour"], "%H:%M"
                ).time(),
                end=datetime.datetime.strptime(
                    time_["ends_at"]["format_24_hour"], "%H:%M"
                ).time(),
            )
            for time_ in times_data
            if time_["spaces"] > 0 and time_["booking"] is None
        ]


    @_requires_authentication
    #@log_method_inputs_and_outputs
    def add_to_cart(self, slot: ActivitySlot) -> ActivityCart:
        """
        Añade un slot de actividad al carrito, imitando el payload
        que envía la web de Better en /api/activities/cart/add.
        """

        payload = {
            "items": [
                {
                    # Estos campos son exactamente los que vimos en DevTools
                    "id": slot.id,
                    "type": slot.cart_type,               # "activity"
                    "pricing_option_id": slot.pricing_option_id,
                    "apply_benefit": True,
                    "activity_restriction_ids": slot.restriction_ids,
                }
            ],
            # En tu navegador aparecen explícitamente como null
            "membership_user_id": None,
            "selected_user_id": None,
        }

        response = self.session.post("activities/cart/add", json=payload)

        if response.status_code != 200:
            # Intentar sacar solo el mensaje corto de error
            try:
                msg = response.json().get("message", "")
            except Exception:
                msg = response.text[:200]  # por si acaso, truncado

            if msg:
                logging.error("No se pudo añadir al carrito: %s", msg)
            else:
                logging.error("No se pudo añadir al carrito (status %s).", response.status_code)

        response.raise_for_status()

        data = response.json()["data"]

        return ActivityCart(
            id=data["id"],
            amount=data["total"],
            source=data["source"],
        )



    @_requires_authentication
    @log_method_inputs_and_outputs
    def checkout_with_benefit(self, cart: ActivityCart) -> int:
        complete_checkout_response = self.session.post(
            "checkout/complete",
            json=dict(
                completed_waivers=[],
                payments=[],
                selected_user_id=None,
                source=cart.source,
                terms=[1],
            ),
        )
        complete_checkout_response.raise_for_status()

        return complete_checkout_response.json()["complete_order_id"]


    def get_raw_slots_for_day(self, venue_slug: str, activity_slug: str, target_date: str):
        """
        Devuelve los slots RAW directamente de la API de Better,
        sin usar ActivitySlot (ya que el modelo NO incluye toda la información).
        """
        url = (
            f"{self.base_url}/api/activities/venue/{venue_slug}"
            f"/activity/{activity_slug}/slots"
        )
        params = {"date": target_date}

        resp = self.session.get(url, params=params)
        resp.raise_for_status()

        data = resp.json()
        return data.get("data", [])