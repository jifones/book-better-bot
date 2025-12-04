import sys
from datetime import datetime, date, time, timezone, timedelta
import os
import argparse
import time
import zoneinfo
import requests

from book_better.better.live_client import LiveBetterClient
from book_better.enums import BetterActivity, BetterVenue
from book_better.utils import parse_time
from supabase_client import (
    get_pending_requests,
    update_request_seen,
    resolve_credentials_for_request,  
    update_request_booked,           
)


def parse_time_str(t: str) -> time:
    # Esperamos formato 'HH:MM:SS'
    return datetime.strptime(t, "%H:%M:%S").time()

def clean_slug(value):
    """
    Limpia un slug que pueda haber llegado con comillas o espacios:
    "'islington-tennis-centre'" -> "islington-tennis-centre"
    """
    if not isinstance(value, str):
        return value
    return value.strip().strip("'\"")

def should_process_request(req: dict, now: datetime) -> str:
    """
    Decide qué hacer con una request según la fecha/hora actual (hora Londres).

    Devuelve uno de:
      - "WAIT_RELEASE" → hoy es el día de liberación (t+7) pero antes de la hora de apertura (p.ej. 22:00 London)
      - "SKIP"         → todavía no toca (antes del día de liberación, o fuera de ventana)
      - "EXPIRE"       → ya pasó la fecha objetivo; marcar como expirada
      - "PROCESS"      → toca procesarla ahora (ventana y condiciones cumplidas)
    """
    # Hora Londres robusta (invierno/verano)
    tz = zoneinfo.ZoneInfo("Europe/London")
    now_lon = now.astimezone(tz)
    today_lon = now_lon.date()
    now_time_lon = now_lon.time()

    # Fechas relevantes
    target_date = date.fromisoformat(req["target_date"])                # día de juego
    search_start_date = date.fromisoformat(req["search_start_date"])    # desde cuándo podemos buscar
    release_date = target_date - timedelta(days=7)                      # día de liberación (t+7)

    # Hora de apertura (por defecto 22:00:00 London; configurable por env RELEASE_TIME="HH:MM:SS")
    hh, mm, ss = map(int, os.environ.get("RELEASE_TIME", "22:00:00").split(":"))
    release_dt = datetime(release_date.year, release_date.month, release_date.day, hh, mm, ss, tzinfo=tz)

    # 0) Si la fecha objetivo ya pasó → expira
    if today_lon > target_date:
        return "EXPIRE"

    # 0.5) Si estamos en t+1 (un día antes de jugar) → cerrar (no quiero seguir buscando)
    if today_lon == (target_date - timedelta(days=1)):
        return "CLOSE"

    # 1) Aún no alcanzamos la fecha mínima desde la que se permite buscar
    if today_lon < search_start_date:
        return "SKIP"

    # 2) Antes del día/hora de liberación:
    #    - Si hoy ES el día de liberación pero antes de la hora → WAIT_RELEASE (el diario esperará)
    #    - Si hoy es anterior al día de liberación → SKIP
    if now_lon < release_dt:
        return "WAIT_RELEASE" if today_lon == release_date else "SKIP"

    # 3) Ya pasó la liberación (válido para t+7 tras la hora de apertura y también t+6, t+5…)
    #    Comprobamos ventana horaria local (London)
    window_start = parse_time_str(req["search_window_start_time"])  # 'HH:MM:SS'
    window_end   = parse_time_str(req["search_window_end_time"])

    if window_start <= now_time_lon <= window_end:
        return "PROCESS"

    # 4) Está en rango de fechas, pero fuera de ventana horaria
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

# ADD: utilidades para hora local y espera
def london_now(tz_name: str = "Europe/London"):
    return datetime.now(zoneinfo.ZoneInfo(tz_name))

def wait_until_local(target_hms: str = "22:00:01", tz_name: str = "Europe/London"):
    """Bloquea hasta HH:MM:SS en la zona tz_name (p.ej. 22:00:01 Europe/London)."""
    tz = zoneinfo.ZoneInfo(tz_name)
    now = datetime.now(tz)
    hh, mm, ss = map(int, target_hms.split(":"))
    target = now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
    if target <= now:
        # si ya pasó, no esperamos (útil en ejecuciones manuales tardías)
        return
    # Espera “mixta”: dormir largo y luego afinar los últimos 60s
    delta = (target - now).total_seconds()
    if delta > 60:
        time.sleep(delta - 60)
    while datetime.now(tz) < target:
        time.sleep(0.2)


def probe_better_slots_for_request(req: dict) -> str:
    try:
        username, password = resolve_credentials_for_request(req)
    except Exception as e:
        return f"ERROR: {e!r}"

    client = LiveBetterClient(username=username, password=password)

    venue_slug_raw = req["venue_slug"]
    activity_slug_raw = req["activity_slug"]

    # 👇 Limpieza defensiva por si en DB llegan con comillas
    venue_slug = clean_slug(venue_slug_raw)
    activity_slug = clean_slug(activity_slug_raw)

    print(f"[Scheduler] DEBUG venue_slug desde DB: {repr(venue_slug_raw)} -> limpio: {repr(venue_slug)}")
    print(f"[Scheduler] DEBUG activity_slug desde DB: {repr(activity_slug_raw)} -> limpio: {repr(activity_slug)}")

    target_date = date.fromisoformat(req["target_date"])

    start_raw = str(req["target_start_time"])   # '19:00:00'
    end_raw = str(req["target_end_time"])       # '20:00:00'
    start_pretty = start_raw[:5]                # '19:00'
    end_pretty = end_raw[:5]                    # '20:00'
    start_str = start_pretty.replace(":", "")   # '1900'
    end_str = end_pretty.replace(":", "")       # '2000'

    try:
        start_time = parse_time(start_str)
        end_time = parse_time(end_str)
    except Exception as e:
        return f"ERROR: fallo parseando horas: {e!r}"

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
            f"BETTER_PROBE_OK: 0 slots para {req['target_date']} "
            f"{start_pretty}-{end_pretty}."
        )

    # Si quieres, aquí podríamos llamar a pick_best_slot_for_request solo para ver:
    chosen_slot, chosen_label = pick_best_slot_for_request(req, slots)

    if chosen_slot is None:
        return (
            f"BETTER_PROBE_OK: {count} slots para {req['target_date']} "
            f"{start_pretty}-{end_pretty}, pero no se pudo elegir cancha."
        )

    return (
        f"BETTER_PROBE_OK: {count} slots para {req['target_date']} "
        f"{start_pretty}-{end_pretty}. SELECTED {chosen_label}."
    )


def book_best_slot_for_request(req: dict) -> str:
    try:
        username, password = resolve_credentials_for_request(req)
    except Exception as e:
        return f"ERROR_CREDENTIALS: {e!r}"

    client = LiveBetterClient(username=username, password=password)

    # 🔥 SLUGS DEBEN VENIR LIMPIOS
    venue_slug_raw = req["venue_slug"]
    activity_slug_raw = req["activity_slug"]

    venue_slug = clean_slug(venue_slug_raw)
    activity_slug = clean_slug(activity_slug_raw)

    target_date = date.fromisoformat(req["target_date"])

    # En la BD los tiempos están como '19:00:00'
    start_raw = str(req["target_start_time"])  
    end_raw = str(req["target_end_time"])      

    start_pretty = start_raw[:5]               
    end_pretty = end_raw[:5]                   

    start_str = start_pretty.replace(":", "")  
    end_str = end_pretty.replace(":", "")      

    try:
        start_time = parse_time(start_str)
        end_time = parse_time(end_str)
    except Exception as e:
        return f"ERROR_BOOKING_TIME_PARSE: {e!r}"

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
    booked_court_name = chosen_label                     # p.ej. "Highbury Fields Tennis Court 11"
    booked_start = f"{start_pretty}:00"                  # 'HH:MM:SS'
    booked_end = f"{end_pretty}:00"                      # 'HH:MM:SS'

    try:
        update_request_booked(
            req["id"],
            booked_court_name=booked_court_name,
            booked_slot_start=booked_start,
            booked_slot_end=booked_end,
            last_error=f"BOOKING_OK: order_id={order_id}",
        )
    except Exception as e:
        # El booking fue OK, pero falló el patch; devolvemos el detalle.
        return f"BOOKING_OK_BUT_PATCH_FAILED: order_id={order_id}; patch_error={e!r}"

    return (
        f"BOOKING_OK: reservado {booked_court_name} para {req['target_date']} "
        f"{start_pretty}-{end_pretty}, order_id={order_id}."
    )



def main() -> int:
    start_run = datetime.now(timezone.utc)
    print(f"[Scheduler] Ejecutando a las {start_run.isoformat()}")

    requests = get_pending_requests(limit=50)
    print(f"[Scheduler] Encontradas {len(requests)} requests PENDING/SEARCHING activas.")

    now = datetime.now(timezone.utc)

    # ADD: esperar a 22:00:01 London si hoy es el día T+7 de alguna request
    # (por defecto activado; se puede parametrizar si quieres)
    TARGET_HMS = os.environ.get("TARGET_TIME_LONDON", "22:00:01")
    TZ_NAME = os.environ.get("TARGET_TZ_NAME", "Europe/London")

    lon_today = london_now(TZ_NAME).date()

    def is_t_plus_7(req) -> bool:
        try:
            tdate = date.fromisoformat(req["target_date"])
        except Exception:
            return False
        return (tdate - lon_today) == timedelta(days=0)  # estamos el mismo día del target_date

    # Si hay al menos una request cuyo target_date == hoy (día t+7 real),
    # esperamos hasta 22:00:01 London ANTES de procesar.
    if any(is_t_plus_7(r) for r in requests):
        print(f"[Scheduler] Hoy coincide con target_date para alguna request; esperando a {TARGET_HMS} {TZ_NAME}…")
        wait_until_local(TARGET_HMS, TZ_NAME)
    else:
        print("[Scheduler] No es día t+7 para ninguna request; no se aplica espera a 22:00:01.")
    now = datetime.now(timezone.utc)

    for req in requests:
        rid = req["id"]
        run_mode = os.environ.get("RUN_MODE", "ANY")
        if run_mode == "RELEASE_ONLY":
            tz = zoneinfo.ZoneInfo("Europe/London")
            now_lon = now.astimezone(tz)
            tgt = date.fromisoformat(req["target_date"])
            release_date = tgt - timedelta(days=7)
            if now_lon.date() != release_date:
                print(f"[Scheduler] (RELEASE_ONLY) Skip {req['id']}: target_date={tgt} (t+7={release_date}), hoy={now_lon.date()}.")
                continue
        action = should_process_request(req, now)

        if action == "EXPIRE":
            print(f"[Scheduler] Marcando como EXPIRED request {rid} (target_date ya pasó).")
            try:
                updated = update_request_seen(
                    rid,
                    new_status="EXPIRED",
                    last_error="EXPIRED: target_date passed",
                )
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

            # Flag para activar o no el booking real
            enable_booking = os.environ.get("ENABLE_BETTER_BOOKING", "").lower() == "true"

            if enable_booking:
                # 🔥 MODO BOOKING REAL
                message = book_best_slot_for_request(req)
                print(f"[Scheduler] Resultado BOOKING para {rid}: {message}")

                # Status válidos en la tabla: PENDING, SEARCHING, BOOKED, EXPIRED, FAILED
                if message.startswith("BOOKING_OK"):
                    new_status = "BOOKED"
                elif message.startswith("BOOKING_NO_SLOTS"):
                    new_status = "SEARCHING"
                elif message.startswith("ERROR_BOOKING_"):
                    new_status = "FAILED"
                else:
                    # fallback defensivo
                    new_status = "FAILED"
            else:
                # 🔍 SOLO RADAR (lo que acabas de ver en el log)
                message = probe_better_slots_for_request(req)
                print(f"[Scheduler] Resultado del radar Better para {rid}: {message}")

                # En modo radar nunca cambiamos a ERROR para no romper el check
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
        elif action == "WAIT_RELEASE":
            print("[Scheduler] Aún no es la hora de apertura; se espera al diario de las 22:00 London.")
            continue

        elif action == "CLOSE":
            # Cerrar en t+1: no seguir buscando
            try:
                updated = update_request_seen(
                    req["id"],
                    new_status="CLOSED",  # <= COMA OBLIGATORIA
                    last_error="AUTO_CLOSED_T+1: no se encontraron canchas dentro del período de liberación.",
                    is_active=False,  # (opcional) descomenta si tu helper acepta este campo
                )
                print(f"[Scheduler] Request {req['id']} marcada CLOSED (t+1).")
            except Exception as e:
                print(f"[Scheduler] Error al cerrar {req['id']}: {e}", file=sys.stderr)
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
