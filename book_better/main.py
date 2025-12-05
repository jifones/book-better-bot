import datetime
import logging
import os
from requests.exceptions import HTTPError

from dotenv import load_dotenv

from book_better.better.live_client import LiveBetterClient
from book_better.enums import BetterActivity, BetterVenue
from book_better.utils import parse_time

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])

ACTIVITY_DATE = datetime.date.today() + datetime.timedelta(days=3)
TARGET_START_TIME = datetime.time(19, 0)
TARGET_END_TIME = datetime.time(20, 0)

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])

load_dotenv()

# Prioridad de canchas (por location_id):
# 11, 10, 9, luego 1 en adelante
COURT_PRIORITY = [
    "5157",  # Court 11
    "5156",  # Court 10
    "5155",  # Court 9
    "5147",  # Court 1
    "5148",  # Court 2
    "5149",  # Court 3
    "5150",  # Court 4
    "5151",  # Court 5
    "5152",  # Court 6
    "5153",  # Court 7
    "5154",  # Court 8
]


def choose_slot_with_court_priority(slots):
    """
    Elige el mejor slot según la prioridad de canchas:
    11, 10, 9, luego 1 en adelante.
    """
    if not slots:
        return None

    def court_rank(slot):
        try:
            return COURT_PRIORITY.index(slot.location_id)
        except ValueError:
            # Si por alguna razón aparece un location_id nuevo, lo mandamos al final.
            return len(COURT_PRIORITY)

    ordered = sorted(slots, key=court_rank)
    best = ordered[0]
    logging.info(
        "✅ Slot elegido por prioridad de cancha: %s (location_id=%s)",
        best,
        best.location_id,
    )
    return best


def book_best_available_slot():
    client = LiveBetterClient(
        username=os.environ["BETTER_USERNAME"], password=os.environ["BETTER_PASSWORD"]
    )

    available_slots = client.get_available_slots_for(
        venue=BetterVenue(os.environ["BETTER_VENUE_SLUG"]),
        activity=BetterActivity(os.environ["BETTER_ACTIVITY_SLUG"]),
        activity_date=ACTIVITY_DATE,
        start_time=parse_time(os.environ["BETTER_ACTIVITY_START_TIME"]),
        end_time=parse_time(os.environ["BETTER_ACTIVITY_END_TIME"]),
    )
    if not available_slots:
        logging.error(
            "Could not find any available slot",
            extra=dict(available_slots=available_slots),
        )
        return

    order_id: int | None = None
    for slot in available_slots:
        try:
            cart = client.add_to_cart(slot)
            order_id = client.checkout_with_benefit(cart)
        except Exception:
            logging.error(
                "Could not book slot, will try booking the next available slot",
                exc_info=True,
                extra=dict(slot=slot),
            )
            continue
        else:
            break

    if order_id is None:
        logging.error(
            "Could not book any slot",
            extra=dict(available_slots=available_slots),
        )
        return

    return order_id

def book_with_credit_for_date(
    target_date: datetime.date,
    start_time: datetime.time,
    end_time: datetime.time,
    better_account: str = "javier",
):
    """
    Intenta reservar con crédito un slot en target_date entre start_time y end_time.
    - Usa la lista de COURT_PRIORITY para elegir la cancha.
    - Si todavía no hay slots (la fecha no está abierta), devuelve status 'not_open_yet'.
    """

    # Elegimos usuario/clave según la cuenta Better
    if better_account == "javier":
        username = os.environ["BETTER_USERNAME_JAVIER"]
        password = os.environ["BETTER_PASSWORD_JAVIER"]
    else:
        # Por ahora, fallback a las variables genéricas si algún día añadimos más cuentas
        username = os.environ["BETTER_USERNAME"]
        password = os.environ["BETTER_PASSWORD"]

    client = LiveBetterClient(username=username, password=password)

    logging.info(
        "Intentando reservar con crédito el %s de %s–%s para el usuario '%s'...",
        target_date,
        start_time.strftime("%H:%M"),
        end_time.strftime("%H:%M"),
        better_account,
    )

    # 1) Pedimos los slots para ese día y franja
    slots = client.get_available_slots_for(
        venue=BetterVenue(os.environ["BETTER_VENUE_SLUG"]),
        activity=BetterActivity(os.environ["BETTER_ACTIVITY_SLUG"]),
        activity_date=target_date,
        start_time=start_time,
        end_time=end_time,
    )

    if not slots:
        # Aquí entra tanto el caso "no hay JSON porque no está abierto" como "data: []"
        logging.info(
            "⏳ Todavía no existe: Better aún no tiene slots abiertos para %s entre %s–%s.",
            target_date,
            start_time.strftime("%H:%M"),
            end_time.strftime("%H:%M"),
        )
        return {"status": "not_open_yet"}

    # 2) Elegimos el mejor slot según la prioridad de canchas
    slot = choose_slot_with_court_priority(slots)
    if slot is None:
        logging.error("No se pudo elegir ningún slot aunque la lista no estaba vacía.")
        return {"status": "no_slot"}

    logging.info("Intentando reservar slot: %s", slot)

    # 3) Añadimos al carrito
    client.add_to_cart(slot)

    # 4) Obtenemos resumen del carrito
    cart = client.get_cart_summary()
    logging.info(
        "Carrito: id=%s total=%s itemHash=%s",
        cart.id,
        cart.total,
        cart.item_hash,
    )

    # Crédito general disponible (lo que vimos en /api/activities/cart)
    general_credit = cart.general_credit_available

    if general_credit < cart.total:
        logging.error(
            "Crédito insuficiente: disponible=%s, necesario=%s",
            general_credit,
            cart.total,
        )
        return None

    # 1) Reservar el crédito
    client.apply_credit(cart.total, cart_source=cart.source)

    # 2) Completar el checkout pagando con ese crédito
    result = client.checkout_with_credit(
        cart_id=cart.id,
        item_hash=cart.item_hash,
        amount=cart.total,
        source=cart.source,
    )

    return result



def main():
    """
    Modo seguro para pruebas locales:
    - Hace login.
    - Pide los times (horas disponibles) para ACTIVITY_DATE.
    - Comprueba si la hora objetivo (TARGET_START_TIME–TARGET_END_TIME) está disponible.
    - NO reserva nada.
    """
    client = LiveBetterClient(
        username=os.environ["BETTER_USERNAME"],
        password=os.environ["BETTER_PASSWORD"],
    )

    client.authenticate()

    times = client.get_available_times_for(
        venue=BetterVenue(os.environ["BETTER_VENUE_SLUG"]),
        activity=BetterActivity(os.environ["BETTER_ACTIVITY_SLUG"]),
        activity_date=ACTIVITY_DATE,
    )

    logging.info("Available times (%d): %s", len(times), times)

    target_available = any(
        t.start == TARGET_START_TIME and t.end == TARGET_END_TIME
        for t in times
    )

    if target_available:
        logging.info(
            "✅ La hora objetivo %s–%s ESTÁ disponible",
            TARGET_START_TIME.strftime("%H:%M"),
            TARGET_END_TIME.strftime("%H:%M"),
        )
    else:
        logging.info(
            "❌ La hora objetivo %s–%s NO está disponible",
            TARGET_START_TIME.strftime("%H:%M"),
            TARGET_END_TIME.strftime("%H:%M"),
        )

    return {
        "status": "ok",
        "times_found": len(times),
        "target_available": target_available,
    }



