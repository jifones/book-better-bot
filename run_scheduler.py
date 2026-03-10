import sys
from datetime import datetime, date, time, timezone, timedelta
import os
import argparse
import time
import zoneinfo
import requests
from requests import HTTPError

from book_better.better.live_client import LiveBetterClient
from book_better.enums import BetterActivity, BetterVenue
from book_better.utils import parse_time
from book_better.main import book_with_credit_for_date
from book_better.models import ActivityCart
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

def _same_str(a: str, b: str) -> bool:
    return (a or "").strip() == (b or "").strip()

def find_consecutive_sibling(curr_req: dict, all_reqs: list[dict]) -> dict | None:
    """
    Busca en 'all_reqs' una request hermana: misma cuenta/fecha/venue/activity,
    activa y en estado pendiente, cuyo inicio == fin de la request actual (bloque contiguo).
    """
    curr_start = (curr_req["target_start_time"] or "")[:5]  # "HH:MM"
    curr_end   = (curr_req["target_end_time"] or "")[:5]

    for r in all_reqs:
        if r["id"] == curr_req["id"]:
            continue
        if not (r.get("is_active") and r.get("status") in ("PENDING","SEARCHING","QUEUED")):
            continue
        if not (
            _same_str(r.get("better_account_id"), curr_req.get("better_account_id"))
            and _same_str(r.get("target_date"), curr_req.get("target_date"))
            and _same_str(r.get("venue_slug"), curr_req.get("venue_slug"))
            and _same_str(r.get("activity_slug"), curr_req.get("activity_slug"))
        ):
            continue

        sib_start = (r["target_start_time"] or "")[:5]
        # Hermana si el siguiente bloque empieza justo cuando termina el actual
        if sib_start == curr_end:
            return r
    return None


def should_process_request(req: dict, now: datetime) -> str:
    """
    Decide qué hacer con una request según la fecha/hora actual (hora Londres).

    Devuelve uno de:
      - "WAIT_RELEASE" → hoy es el día de liberación (t+7) pero antes de la hora de apertura (p.ej. 22:00 London)
      - "CLOSE"        → estamos en t+1 (un día antes del target_date): cerrar y no seguir buscando
      - "SKIP"         → todavía no toca (antes del día de liberación, o fuera de ventana en modo diario)
      - "EXPIRE"       → ya pasó la fecha objetivo; marcar como expirada
      - "PROCESS"      → toca procesarla ahora
    """
    tz = zoneinfo.ZoneInfo("Europe/London")
    now_lon = now.astimezone(tz)
    today_lon = now_lon.date()
    now_time_lon = now_lon.time()

    target_date = date.fromisoformat(req["target_date"])
    search_start_date = date.fromisoformat(req["search_start_date"])
    release_date = target_date - timedelta(days=7)

    # Hora de apertura (por defecto 22:00:00 London; configurable)
    hh, mm, ss = map(int, os.environ.get("RELEASE_TIME", "22:00:00").split(":"))
    release_dt = datetime(release_date.year, release_date.month, release_date.day, hh, mm, ss, tzinfo=tz)

    # 0) Si ya pasó la fecha objetivo → EXPIRE
    if today_lon > target_date:
        return "EXPIRE"

    # 0.5) Si estamos en t+1 (un día antes de jugar) → CLOSE (no seguir buscando)
    if today_lon == (target_date - timedelta(days=1)):
        return "CLOSE"

    # 1) Aún no alcanza la fecha mínima desde la que se permite buscar → SKIP
    if today_lon < search_start_date:
        return "SKIP"

    # 2) Día de liberación (t+7)
    if today_lon == release_date:
        if now_lon < release_dt:
            # Antes de la hora de apertura
            return "WAIT_RELEASE"
        # Después de la hora de apertura: PROCESS (tanto diario como hourly)
        return "PROCESS"

    # 3) No es t+7 (t+6, t+5, ...): comportamiento depende del modo
    run_mode = os.environ.get("RUN_MODE", "ANY")
    if run_mode == "ANY":
        # Hourly → ignora ventana, permite cazar cancelaciones todo el día
        return "PROCESS"

    # 4) Modo diario (RELEASE_ONLY) fuera de t+7 → SKIP
    # (El filtro extra de t+7 ya lo haces en main() antes de llamar a esta función)
    # Pero si llega aquí por algún motivo, aplicamos ventana como salvaguarda:
    window_start = parse_time_str(req["search_window_start_time"])  # 'HH:MM:SS'
    window_end   = parse_time_str(req["search_window_end_time"])
    if window_start <= now_time_lon <= window_end:
        return "PROCESS"

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

def build_slot_candidates_for_request(req: dict, slots: list, forced_court_number: str | None = None) -> list:
    """
    Construye la lista de candidatos en orden:
    1) forced_court_number si viene informado
    2) preferred_court_name_1 / 2 / 3
    3) resto de slots conocidos
    4) slots sin número claro
    """
    prefs_raw = [
        req.get("preferred_court_name_1"),
        req.get("preferred_court_name_2"),
        req.get("preferred_court_name_3"),
    ]
    preferred_numbers: list[str] = []

    if forced_court_number:
        preferred_numbers.append(str(forced_court_number).strip())

    for pref in prefs_raw:
        num = extract_court_number_from_string(pref) if pref else None
        if num and num not in preferred_numbers:
            preferred_numbers.append(num)

    slots_by_court: dict[str, list] = {}
    unknown_slots: list = []

    for s in slots:
        num = get_slot_court_number(s)
        if num:
            slots_by_court.setdefault(num, []).append(s)
        else:
            unknown_slots.append(s)

    candidates: list = []
    used_ids: set[int] = set()

    for pref_num in preferred_numbers:
        for s in slots_by_court.get(pref_num, []):
            if s.id not in used_ids:
                candidates.append(s)
                used_ids.add(s.id)

    for _, group in slots_by_court.items():
        for s in group:
            if s.id not in used_ids:
                candidates.append(s)
                used_ids.add(s.id)

    for s in unknown_slots:
        if s.id not in used_ids:
            candidates.append(s)
            used_ids.add(s.id)

    return candidates

def extract_booked_court_number_from_message(message: str) -> str | None:
    """
    Intenta sacar el número de cancha desde mensajes tipo:
    'BOOKING_OK: reservado Court 10 para 2026-03-12 19:00-20:00, order_id=...'
    """
    if not message:
        return None

    marker = "reservado Court "
    if marker in message:
        tail = message.split(marker, 1)[1]
        digits = "".join(ch for ch in tail if ch.isdigit())
        return digits or None

    return None

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


def book_best_slot_for_request(req: dict, forced_court_number: str | None = None) -> str:
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

    # 2) Ordenar candidatos: forced court primero, luego preferidas, luego el resto
    candidates = build_slot_candidates_for_request(
        req,
        slots,
        forced_court_number=forced_court_number,
    )

    prefs_log = [
        req.get("preferred_court_name_1"),
        req.get("preferred_court_name_2"),
        req.get("preferred_court_name_3"),
    ]
    print(
        f"[Booking] Request {req['id']} preferencias={prefs_log} | forced_court_number={forced_court_number}"
    )
    print(
        f"[Booking] Request {req['id']} candidates_total={len(candidates)}"
    )

    for idx, cand in enumerate(candidates, start=1):
        cand_num = get_slot_court_number(cand)
        cand_label = f"Court {cand_num}" if cand_num else getattr(cand, 'name', 'unknown')
        print(
            f"[Booking] Candidate #{idx} | slot_id={cand.id} | court={cand_label} | location_id={cand.location_id}"
        )

    # 3) Intentar reservar: si un slot queda "already full", probamos el siguiente
    max_attempts = min(8, len(candidates))
    last_full_msg = None

    for i in range(max_attempts):
        chosen_slot = candidates[i]
        chosen_num = get_slot_court_number(chosen_slot)
        chosen_label = f"Court {chosen_num}" if chosen_num else getattr(chosen_slot, "name", "unknown")

        print(
            f"[Booking] Intentando request {req['id']} | slot_id={chosen_slot.id} | court={chosen_label} | intento={i+1}/{max_attempts}"
        )

        try:
            # idempotencia: si ya está en el carrito, no lo agregamos de nuevo
            if client.cart_contains_slot_id(chosen_slot.id):
                print(
                    f"[Booking] Slot ya estaba en carrito | request {req['id']} | slot_id={chosen_slot.id} | court={chosen_label}"
                )
                summary = client.get_cart_summary()
                cart = ActivityCart(id=summary.id, amount=summary.total, source=summary.source)
            else:
                cart = client.add_to_cart(chosen_slot)

        except HTTPError as e:
            # mensaje de Better
            try:
                msg = (e.response.json() or {}).get("message", "") if e.response is not None else ""
            except Exception:
                msg = ""

            if "already full" in (msg or "").lower():
                last_full_msg = msg
                print(
                    f"[Booking] Slot full al add_to_cart | request {req['id']} | slot_id={chosen_slot.id} | court={chosen_label} | motivo={msg}"
                )
                continue  # PROBAR SIGUIENTE SLOT

            return (
                f"ERROR_BOOKING_ADD_TO_CART: {msg or repr(e)} "
                f"para {req['target_date']} {start_pretty}-{end_pretty} ({chosen_label})."
            )
        
        # 4) checkout pagando con CRÉDITOS (igual que el navegador: /credits/apply + /checkout/complete)
        try:
            # Leemos el total e item_hash actual del carrito
            summary = client.get_cart_summary()
            amount = int(summary.total or 0)

            if amount <= 0:
                return (
                    f"ERROR_BOOKING_CHECKOUT: carrito con total=0 "
                    f"para {req['target_date']} {start_pretty}-{end_pretty} ({chosen_label})."
                )

            # Validación: si no hay crédito suficiente, no tiene sentido seguir con checkout
            if summary.general_credit_available < amount or summary.general_credit_max_applicable < amount:
                return (
                    f"BOOKING_NO_CREDIT: crédito insuficiente para {req['target_date']} "
                    f"{start_pretty}-{end_pretty} ({chosen_label}). "
                    f"need={amount}, avail={summary.general_credit_available}, max={summary.general_credit_max_applicable}"
                )

            # Paso 1: reservar crédito por el monto del carrito
            client.apply_credit(amount=amount, cart_source=summary.source)

            # Paso 2: refrescar el carrito (item_hash puede cambiar después de apply)
            summary2 = client.get_cart_summary()

            # Paso 3: completar checkout usando tender_type=credit
            complete = client.checkout_with_credit(
                cart_id=summary2.id,
                item_hash=summary2.item_hash,
                amount=amount,
                source=summary2.source,
            )

            order_id = complete.get("complete_order_id") if isinstance(complete, dict) else None

        except HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 422:
                if client.cart_contains_slot_id(chosen_slot.id):
                    return (
                        f"BOOKING_IN_CART: checkout_422 para {req['target_date']} "
                        f"{start_pretty}-{end_pretty} ({chosen_label})."
                    )
            return f"ERROR_BOOKING_CHECKOUT: {e!r}"
        
        except Exception as e:
            return f"ERROR_BOOKING_CHECKOUT: {e!r}"

        if not order_id:
            return (
                "ERROR_BOOKING_CHECKOUT: checkout sin complete_order_id "
                f"para {req['target_date']} {start_pretty}-{end_pretty} ({chosen_label})."
            )

        # ✅ ÉXITO REAL: en cuanto tenemos order_id, parchamos la request y salimos
        booked_court_name = chosen_label
        booked_start = datetime.combine(
            target_date,
            datetime.strptime(start_pretty, "%H:%M").time(),
            tzinfo=zoneinfo.ZoneInfo("Europe/London"),
        ).isoformat()
        booked_end = datetime.combine(
            target_date,
            datetime.strptime(end_pretty, "%H:%M").time(),
            tzinfo=zoneinfo.ZoneInfo("Europe/London"),
        ).isoformat()

        print(
            f"[Booking] Reserva OK | request {req['id']} | court={booked_court_name} | slot_id={chosen_slot.id} | order_id={order_id}"
        )

        try:
            update_request_booked(
                req["id"],
                booked_court_name=booked_court_name,
                booked_slot_start=booked_start,
                booked_slot_end=booked_end,
                last_error=f"BOOKING_OK: order_id={order_id}",
            )
        except Exception as e:
            return f"BOOKING_OK_BUT_PATCH_FAILED: order_id={order_id}; patch_error={e!r}"

        return (
            f"BOOKING_OK: reservado {booked_court_name} para {req['target_date']} "
            f"{start_pretty}-{end_pretty}, order_id={order_id}."
        )

    if last_full_msg:
        return (
            f"BOOKING_NO_SLOTS: todos los candidatos terminaron full para {req['target_date']} "
            f"{start_pretty}-{end_pretty}. last_full={last_full_msg}"
        )

    return (
        f"BOOKING_NO_SLOTS: sin candidatos reservables para {req['target_date']} "
        f"{start_pretty}-{end_pretty}."
    )

def book_with_credit_for_request(req: dict) -> str:
    """
    Usa el flujo legacy de crédito, inyectando alias BETTER_USERNAME/BETTER_PASSWORD
    a partir de las env-keys que ya resuelve la fila (u_key/p_key).
    """
    # 1) Normaliza fecha/hora
    tgt_date = date.fromisoformat(req["target_date"])
    start = parse_time(req["target_start_time"].replace(":", "")[:4])  # "HH:MM:SS" -> "HHMM"
    end   = parse_time(req["target_end_time"].replace(":", "")[:4])    # "HH:MM:SS" -> "HHMM"

    # 2) Resuelve las env-keys (no el valor)
    u_key, p_key = resolve_credentials_for_request(req)

    # 3) Obtén el valor real; si no existe esa clave, cae a los alias genéricos
    u_val = os.environ.get(u_key) or os.environ.get("BETTER_USERNAME")
    p_val = os.environ.get(p_key) or os.environ.get("BETTER_PASSWORD")

    if not u_val or not p_val:
        return f"ERROR_BOOKING_CHECKOUT_MISSING_ENV: {u_key}/{p_key} + BETTER_USERNAME/BETTER_PASSWORD no están presentes en env."

    # Setea alias SOLO para esta ejecución del proceso
    os.environ["BETTER_USERNAME"] = u_val
    os.environ["BETTER_PASSWORD"] = p_val
    label = u_key.split("BETTER_USERNAME_", 1)[1].lower() if u_key.upper().startswith("BETTER_USERNAME_") else "javier"
    better_account = label

    # --- NUEVO: inyecta slugs que main.py espera ---
    venue_slug = (req.get("venue_slug") or "").strip()
    activity_slug = (req.get("activity_slug") or "").strip()
    if not venue_slug or not activity_slug:
        return "ERROR_BOOKING_CHECKOUT_MISSING_SLUGS: venue_slug/activity_slug vacíos en la request"

    os.environ["BETTER_VENUE_SLUG"] = venue_slug
    os.environ["BETTER_ACTIVITY_SLUG"] = activity_slug

    # (opcional, sólo si tu main.py los usa para priorizar canchas)
    pref1 = (req.get("preferred_court_name_1") or "").strip()
    pref2 = (req.get("preferred_court_name_2") or "").strip()
    pref3 = (req.get("preferred_court_name_3") or "").strip()
    if pref1:
        os.environ["BETTER_PREF_COURT_1"] = pref1
    if pref2:
        os.environ["BETTER_PREF_COURT_2"] = pref2
    if pref3:
        os.environ["BETTER_PREF_COURT_3"] = pref3

    os.environ["BETTER_TARGET_DATE"] = tgt_date.isoformat()            # "YYYY-MM-DD"
    os.environ["BETTER_START_HHMM"]  = start.strftime("%H%M")          # "HHMM"
    os.environ["BETTER_END_HHMM"]    = end.strftime("%H%M")            # "HHMM"

    # 5) Ejecuta el flujo legacy de crédito
    try:
        result = book_with_credit_for_date(
            target_date=tgt_date,
            start_time=start,
            end_time=end,
            better_account=better_account,
        )
    except KeyError as e:
        # Si main.py pide otra env, lo verás por nombre exacto
        return f"ERROR_BOOKING_CHECKOUT_MISSING_ENVVAR: {e.args[0]} required by main.py"
    except Exception as e:
        return f"ERROR_BOOKING_CHECKOUT: {e}"

    # 6) Normaliza el mensaje esperado por tu scheduler
    release_date = tgt_date - timedelta(days=7)

    if isinstance(result, dict):
        st = result.get("status")

        # Si ya pasó el día de liberación (t+7) pero el flujo dice "not_open_yet",
        # lo tratamos como "no_slot" para evitar el mensaje confuso.
        if st == "not_open_yet" and date.today() > release_date:
            return f"BOOKING_NO_SLOTS: 0 slots for {req['target_date']} {req['target_start_time'][:5]}-{req['target_end_time'][:5]}"
        if st == "not_open_yet":
            return f"BOOKING_NO_SLOTS: not_open_yet for {req['target_date']} {req['target_start_time'][:5]}-{req['target_end_time'][:5]}"
        if st == "no_slot":
            return f"BOOKING_NO_SLOTS: 0 slots for {req['target_date']} {req['target_start_time'][:5]}-{req['target_end_time'][:5]}"
        if st == "ok":
            return "BOOKING_OK: credit checkout completed"

    if result:
        return "BOOKING_OK: credit checkout completed"

    return "ERROR_BOOKING_CHECKOUT: credit flow returned empty/None"


def main() -> int:
    start_run = datetime.now(timezone.utc)
    print(f"[Scheduler] Ejecutando a las {start_run.isoformat()}")

    # --- Guard de horario sólo para el HOURLY ---
    if os.environ.get("RUN_MODE") == "ANY":
        tz = zoneinfo.ZoneInfo("Europe/London")
        now_lon = datetime.now(tz)
        h = now_lon.hour
        # Permitido: 07–23 Londres, EXCEPTUANDO 20 y 21
        if not (7 <= h <= 23) or h in (20, 21):
            print("[Scheduler] Hourly: fuera de ventana (permitido 07–23 London, excl. 20–21). Salgo.")
            sys.exit(0)

    requests = get_pending_requests(limit=50)
    print(f"[Scheduler] Encontradas {len(requests)} requests PENDING/SEARCHING activas.")

    # --- ESPERA EXCLUSIVA PARA EL DIARIO ---
    # Si corremos en modo diario (RELEASE_ONLY) y no nos han pedido saltar la espera,
    # bloqueamos hasta la hora objetivo en Londres antes de procesar NADA.
    if os.environ.get("RUN_MODE") == "RELEASE_ONLY" and os.environ.get("SKIP_WAIT", "0") != "1":
        target_hms = os.environ.get("TARGET_TIME_LONDON", "22:00:01")
        tz_name = os.environ.get("TARGET_TZ_NAME", "Europe/London")
        print(f"[Scheduler] Diario: esperando hasta {target_hms} {tz_name} antes de procesar…")
        wait_until_local(target_hms, tz_name)

    # Recalcula 'now' después de la espera (o sin esperar en hourly)
    now = datetime.now(timezone.utc)
    processed_ids: set[str] = set()

    for req in requests:
        rid = req["id"]
        if rid in processed_ids:
            print(f"[Scheduler] Skip {rid}: ya procesada en este run.")
            continue
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
                # 🔥 COMPRA REAL usando el flujo nuevo con LiveBetterClient
                #     → esto además actualiza booked_court_name / booked_slot_start / booked_slot_end
                message = book_best_slot_for_request(req)
                print(f"[Scheduler] Resultado BOOKING para {rid}: {message}")

                # Reintento 1× con el MISMO flujo si el checkout falló
                if message.startswith("ERROR_BOOKING_CHECKOUT") and "422" not in message:
                    print("[Scheduler] checkout error (non-422): retrying once…")
                    message = book_best_slot_for_request(req)
                    print(f"[Scheduler] Resultado BOOKING (retry) para {rid}: {message}")

                # Mapeo de estado (igual que antes)
                if message.startswith("BOOKING_OK"):
                    new_status = "BOOKED"
                elif message.startswith("BOOKING_IN_CART"):
                    new_status = "SEARCHING"   # seguimos intentando (y evitamos romper constraint)
                elif message.startswith("BOOKING_NO_SLOTS"):
                    new_status = "SEARCHING"
                elif message.startswith("BOOKING_NO_CREDIT"):
                    new_status = "SEARCHING"
                elif message.startswith("ERROR_BOOKING_SLOTS") and any(code in message for code in (" 500", " 502", " 503", " 504")):
                    new_status = "SEARCHING"  # Better caído / inestable: seguir intentando
                elif message.startswith("ERROR_BOOKING_CHECKOUT") and "422" in message:
                    new_status = "SEARCHING"   # idem
                elif message.startswith("ERROR_BOOKING_"):
                    new_status = "FAILED"
                else:
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

            # Encadenar bloque contiguo (solo si el primero quedó BOOKED)
            if new_status == "BOOKED":
                sib = find_consecutive_sibling(req, requests)
                if sib:
                    first_court_number = extract_booked_court_number_from_message(message)
                    print(
                        f"[Scheduler] Intentando bloque contiguo para {sib['id']} ({sib['target_start_time'][:5]}-{sib['target_end_time'][:5]})… "
                        f"forced_court_number={first_court_number}"
                    )
                    msg2 = book_best_slot_for_request(
                        sib,
                        forced_court_number=first_court_number,
                    )

                    if msg2.startswith("BOOKING_OK"):
                        try:
                            update_request_seen(
                                sib["id"],
                                new_status="BOOKED",
                                last_error=msg2,
                            )
                            print(f"[Scheduler] Segundo bloque BOOKED (request {sib['id']}).")
                            processed_ids.add(sib["id"])
                        except Exception as e:
                            print(f"[Scheduler] Error al actualizar segundo bloque {sib['id']}: {e}", file=sys.stderr)

                    elif msg2.startswith("BOOKING_NO_SLOTS"):
                        try:
                            update_request_seen(
                                sib["id"],
                                new_status="SEARCHING",
                                last_error=msg2,
                            )
                            print(f"[Scheduler] Segundo bloque sin cupos (request {sib['id']}).")
                            processed_ids.add(sib["id"])
                        except Exception as e:
                            print(f"[Scheduler] Error al marcar SEARCHING el segundo bloque {sib['id']}: {e}", file=sys.stderr)

                    else:
                        # Mapeo “seguro” para el segundo bloque (igual que el primero)
                        if msg2.startswith("BOOKING_IN_CART"):
                            st2 = "SEARCHING"
                        elif msg2.startswith("BOOKING_NO_CREDIT"):
                            st2 = "SEARCHING"   # como tú quieres
                        elif msg2.startswith("BOOKING_NO_SLOTS"):
                            st2 = "SEARCHING"
                        elif msg2.startswith("ERROR_BOOKING_SLOTS") and any(code in msg2 for code in (" 500", " 502", " 503", " 504")):
                            st2 = "SEARCHING"
                        elif msg2.startswith("ERROR_BOOKING_CHECKOUT") and "422" in msg2:
                            st2 = "SEARCHING"
                        elif msg2.startswith("ERROR_BOOKING_"):
                            st2 = "FAILED"
                        else:
                            st2 = "FAILED"

                        try:
                            update_request_seen(
                                sib["id"],
                                new_status=st2,
                                last_error=msg2,
                            )
                            print(f"[Scheduler] Segundo bloque {st2} (request {sib['id']}): {msg2}")
                            processed_ids.add(sib["id"])
                        except Exception as e:
                            print(f"[Scheduler] Error al actualizar segundo bloque {sib['id']}: {e}", file=sys.stderr)



        elif action == "WAIT_RELEASE":
            if os.environ.get("RUN_MODE") == "RELEASE_ONLY":
                print("[Scheduler] Aún no es la hora de apertura; se espera al diario de las 22:00 London.")
            # En hourly (RUN_MODE=ANY) no decimos nada; simplemente lo saltamos
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
