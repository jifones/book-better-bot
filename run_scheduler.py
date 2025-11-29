import sys
from datetime import datetime, date, time, timezone
import os

from book_better.better.live_client import LiveBetterClient
from book_better.enums import BetterActivity, BetterVenue
from book_better.utils import parse_time
from supabase_client import get_pending_requests, update_request_seen


def parse_time_str(t: str) -> time:
    # Esperamos formato 'HH:MM:SS'
    return datetime.strptime(t, "%H:%M:%S").time()


def should_process_request(req: dict, now: datetime) -> str:
    """
    Decide qué hacer con una request según la fecha/hora actual.

    Devuelve:
      - "SKIP"     → todavía no toca procesarla
      - "EXPIRE"   → ya pasó la fecha objetivo, hay que marcarla EXPIRED
      - "PROCESS"  → toca procesarla ahora (dentro de la ventana horaria)
    """
    today = now.date()
    now_time = now.time()

    target_date = date.fromisoformat(req["target_date"])
    search_start_date = date.fromisoformat(req["search_start_date"])

    # Si aún no ha llegado la fecha desde la que podemos buscar → no hacemos nada
    if today < search_start_date:
        return "SKIP"

    # Si ya se pasó la fecha objetivo → marcamos como expirada
    if today > target_date:
        return "EXPIRE"

    # Si es el día objetivo (o posterior a search_start_date) y estamos dentro
    # de la ventana horaria, la procesamos.
    window_start = parse_time_str(req["search_window_start_time"])
    window_end = parse_time_str(req["search_window_end_time"])

    if window_start <= now_time <= window_end:
        return "PROCESS"

    # Está en rango de fechas, pero fuera de la ventana horaria
    return "SKIP"

def extract_court_number_from_string(text: str) -> str | None:
    """
    Extrae el número de cancha desde un texto como:
    - 'Court 5'
    - 'Highbury Fields Tennis Court 7'
    - 'highbury-fields-tennis-court-11'
    Devuelve '5', '7', '11' o None si no hay dígitos.
    """
    if not text:
        return None
    digits = "".join(ch for ch in str(text) if ch.isdigit())
    return digits or None


def get_slot_court_number(slot) -> str | None:
    """
    Intenta sacar el número de cancha desde el slot de Better.
    Ahora mismo usamos slot.name (slug como 'highbury-fields-tennis-court-7').
    """
    name = getattr(slot, "name", "") or ""
    return extract_court_number_from_string(name)


def pick_best_slot_for_request(req: dict, slots: list):
    """
    Elige el mejor slot según las preferencias de cancha de la request.
    - Usa preferred_court_name_1, 2, 3 (pueden ser 'Court 5' o nombres largos).
    - Si ninguna preferencia coincide, devuelve simplemente el primer slot.
    Devuelve (slot_elegido, court_label) donde court_label es algo tipo 'Court 5'.
    """
    if not slots:
        return None, None

    # 1) Construir lista de preferencias de cancha en forma de números ['5', '3', ...]
    prefs_raw = [
        req.get("preferred_court_name_1"),
        req.get("preferred_court_name_2"),
        req.get("preferred_court_name_3"),
    ]
    preferred_numbers: list[str] = []
    for pref in prefs_raw:
        num = extract_court_number_from_string(pref) if pref else None
        if num:
            preferred_numbers.append(num)

    # 2) Intentar encontrar un slot que coincida con la primera preferencia disponible
    if preferred_numbers:
        # Construimos un dict court_number -> [slots...]
        slots_by_court: dict[str, list] = {}
        for s in slots:
            num = get_slot_court_number(s)
            if not num:
                continue
            slots_by_court.setdefault(num, []).append(s)

        for pref_num in preferred_numbers:
            if pref_num in slots_by_court and slots_by_court[pref_num]:
                chosen = slots_by_court[pref_num][0]
                return chosen, f"Court {pref_num}"

    # 3) Si no hay preferencias o no coinciden, devolvemos el primer slot disponible
    fallback = slots[0]
    fb_num = get_slot_court_number(fallback)
    if fb_num:
        return fallback, f"Court {fb_num}"
    else:
        # último recurso: no tenemos número claro, devolvemos el nombre interno
        name = getattr(fallback, "name", "unknown")
        return fallback, name


def probe_better_slots_for_request(req: dict) -> str:
    """
    Llama a Better para ver cuántos slots hay para esta request.
    Devuelve un string resumen que guardaremos en last_error.
    NO reserva nada, solo mira y elige la mejor cancha según preferencias.
    """
    # 1) Credenciales (de momento, fijas: Javier)
    username = os.environ.get("BETTER_USERNAME_JAVIER")
    password = os.environ.get("BETTER_PASSWORD_JAVIER")

    if not username or not password:
        return "ERROR: faltan BETTER_USERNAME_JAVIER o BETTER_PASSWORD_JAVIER en las variables de entorno."

    client = LiveBetterClient(username=username, password=password)

    # 2) Parámetros de la request
    venue_slug = req["venue_slug"]
    activity_slug = req["activity_slug"]
    target_date = date.fromisoformat(req["target_date"])

    # En la BD los tiempos están como '19:00:00'
    start_raw = str(req["target_start_time"])    # '19:00:00'
    end_raw = str(req["target_end_time"])        # '20:00:00'

    # Para logs bonitos:
    start_pretty = start_raw[:5]                 # '19:00'
    end_pretty = end_raw[:5]                     # '20:00'

    # parse_time espera 'HHMM' (sin dos puntos)
    start_str = start_pretty.replace(":", "")    # '1900'
    end_str = end_pretty.replace(":", "")        # '2000'

    start_time = parse_time(start_str)
    end_time = parse_time(end_str)

    try:
        slots = client.get_available_slots_for(
            venue=BetterVenue(venue_slug),
            activity=BetterActivity(activity_slug),
            activity_date=target_date,
            start_time=start_time,
            end_time=end_time,
        )
    except Exception as e:
        # Devolvemos el error en texto para guardarlo en last_error
        return f"ERROR: fallo al consultar Better: {e!r}"

    count = len(slots)

    if not slots:
        return (
            f"BETTER_PROBE_OK: 0 slots para {req['target_date']} "
            f"{start_pretty}-{end_pretty}."
        )

    # 👉 Nuevo: elegir el mejor slot según preferencias
    chosen_slot, chosen_label = pick_best_slot_for_request(req, slots)

    if chosen_slot is None:
        # muy raro, pero por si acaso
        return (
            f"BETTER_PROBE_OK: {count} slots para {req['target_date']} "
            f"{start_pretty}-{end_pretty}, pero no se pudo elegir cancha."
        )

    # Mensaje final: contamos slots y decimos cuál se seleccionaría
    return (
        f"BETTER_PROBE_OK: {count} slots para {req['target_date']} "
        f"{start_pretty}-{end_pretty}. SELECTED {chosen_label}."
    )

def book_best_slot_for_request(req: dict) -> str:
    """
    Intenta reservar la mejor cancha según las preferencias de la request.
    Usa LiveBetterClient.add_to_cart + checkout_with_benefit.
    Devuelve un mensaje tipo:
      - 'BOOKING_OK: reservado Court 5 ... order_id=123456'
      - 'BOOKING_NO_SLOTS: 0 slots para ...'
      - 'ERROR_BOOKING_...: ...'
    """

    username = os.environ.get("BETTER_USERNAME_JAVIER")
    password = os.environ.get("BETTER_PASSWORD_JAVIER")

    if not username or not password:
        return "ERROR_BOOKING_CONFIG: faltan BETTER_USERNAME_JAVIER o BETTER_PASSWORD_JAVIER."

    client = LiveBetterClient(username=username, password=password)

    venue_slug = req["venue_slug"]
    activity_slug = req["activity_slug"]
    target_date = date.fromisoformat(req["target_date"])

    # En la BD los tiempos están como '19:00:00'
    start_raw = str(req["target_start_time"])    # '19:00:00'
    end_raw = str(req["target_end_time"])        # '20:00:00'

    start_pretty = start_raw[:5]                 # '19:00'
    end_pretty = end_raw[:5]                     # '20:00'

    # parse_time espera 'HHMM'
    start_str = start_pretty.replace(":", "")    # '1900'
    end_str = end_pretty.replace(":", "")        # '2000'

    try:
        start_time = parse_time(start_str)
        end_time = parse_time(end_str)
    except Exception as e:
        return f"ERROR_BOOKING_TIME_PARSE: {e!r}"

    # 1) Obtener slots disponibles en esa franja
    try:
        slots = client.get_available_slots_for(
            venue=BetterVenue(venue_slug),
            activity=BetterActivity(activity_slug),
            activity_date=target_date,
            start_time=start_time,
            end_time=end_time,
        )
    except Exception as e:
        return f"ERROR_BOOKING_SLOTS: fallo al consultar Better: {e!r}"

    count = len(slots)
    if not slots:
        return (
            f"BOOKING_NO_SLOTS: 0 slots para {req['target_date']} "
            f"{start_pretty}-{end_pretty}."
        )

    # 2) Elegir la mejor cancha según preferencias
    chosen_slot, chosen_label = pick_best_slot_for_request(req, slots)
    if chosen_slot is None:
        return (
            f"ERROR_BOOKING_SELECTION: {count} slots para {req['target_date']} "
            f"{start_pretty}-{end_pretty}, pero no se pudo elegir cancha."
        )

    # 3) add_to_cart
    try:
        cart = client.add_to_cart(chosen_slot)
    except Exception as e:
        return f"ERROR_BOOKING_ADD_TO_CART: {e!r}"

    # 4) checkout usando beneficio / crédito
    try:
        order_id = client.checkout_with_benefit(cart)
    except Exception as e:
        return f"ERROR_BOOKING_CHECKOUT: {e!r}"

    if not order_id:
        return (
            "ERROR_BOOKING_CHECKOUT: checkout sin order_id "
            f"para {req['target_date']} {start_pretty}-{end_pretty} ({chosen_label})."
        )

    # 5) Éxito
    return (
        f"BOOKING_OK: reservado {chosen_label} para {req['target_date']} "
        f"{start_pretty}-{end_pretty}, order_id={order_id}."
    )


def main() -> int:
    start_run = datetime.now(timezone.utc)
    print(f"[Scheduler] Ejecutando a las {start_run.isoformat()}")

    requests = get_pending_requests(limit=50)
    print(f"[Scheduler] Encontradas {len(requests)} requests PENDING/SEARCHING activas.")

    now = datetime.now(timezone.utc)

    for req in requests:
        rid = req["id"]
        action = should_process_request(req, now)

        if action == "EXPIRE":
            print(f"[Scheduler] Marcando como EXPIRED request {rid} (target_date ya pasó).")
            try:
                updated = update_request_seen(rid, new_status="EXPIRED")
                print(
                    f"[Scheduler] Request {rid} actualizada a EXPIRED "
                    f"(attempt_count={updated['attempt_count']}, "
                    f"last_run_at={updated['last_run_at']})."
                )
            except Exception as e:
                print(f"[Scheduler] Error al actualizar {rid}: {e}", file=sys.stderr)
                continue

        elif action == "SKIP":
            print(f"[Scheduler] SKIP request {rid} (todavía no toca o fuera de ventana).")
            continue

        elif action == "PROCESS":
            print(f"[Scheduler] >>> Toca procesar request {rid} ahora mismo.")

            # Nuevo flag: solo reservamos de verdad si ENABLE_BETTER_BOOKING == 'true'
            enable_booking = os.environ.get("ENABLE_BETTER_BOOKING", "").lower() == "true"

            if enable_booking:
                # 🔥 MODO RESERVA REAL
                message = book_best_slot_for_request(req)
                print(f"[Scheduler] Resultado BOOKING para {rid}: {message}")

                if message.startswith("BOOKING_OK"):
                    new_status = "BOOKED"
                elif message.startswith("BOOKING_NO_SLOTS"):
                    # No hay slots ahora mismo → seguimos en SEARCHING
                    new_status = "SEARCHING"
                elif message.startswith("ERROR_BOOKING_"):
                    new_status = "ERROR"
                else:
                    # fallback genérico
                    new_status = "ERROR"
            else:
                # 🔍 MODO RADAR SOLO LECTURA (lo que ya tenías)
                message = probe_better_slots_for_request(req)
                print(f"[Scheduler] Resultado del radar Better para {rid}: {message}")

                if message.startswith("ERROR"):
                    new_status = "ERROR"
                else:
                    new_status = "SEARCHING"

            try:
                updated = update_request_seen(
                    rid,
                    new_status=new_status,
                    last_error=message,
                )
                print(
                    f"[Scheduler] Request {rid} actualizada "
                    f"(attempt_count={updated['attempt_count']}, "
                    f"last_run_at={updated['last_run_at']})."
                )
            except Exception as e:
                print(f"[Scheduler] Error al actualizar {rid}: {e}", file=sys.stderr)
                continue


    end_run = datetime.now(timezone.utc)
    elapsed = (end_run - start_run).total_seconds()
    print(
        f"[Scheduler] Fin de la ejecución. "
        f"Duración total: {elapsed:.3f} segundos."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
