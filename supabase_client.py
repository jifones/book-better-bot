import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import time
import requests
from dotenv import load_dotenv
from supabase import Client, create_client

# Cargar .env
load_dotenv()

# Usamos exactamente las env que tienes:
# VITE_SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY
SUPABASE_URL = os.environ.get("VITE_SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
    raise RuntimeError("Faltan SUPABASE_URL / SUPABASE_SERVICE_ROLE (o VITE_SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY) en envs.")

client: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)

# alias usado por get_pending_requests:
supabase = client

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError(
        "Faltan VITE_SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en el .env "
        "(en la repo de better)."
    )

REST_URL = SUPABASE_URL.rstrip("/") + "/rest/v1"

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
}


def get_pending_requests(limit: int = 50, max_retries: int = 3):
    """
    Lee las requests activas que el scheduler puede procesar.
    Retry defensivo ante 5xx/Cloudflare para evitar caídas espurias.
    """
    select_cols = (
        "id,better_account_id,profile_id,"
        "venue_slug,activity_slug,"
        "target_date,target_start_time,target_end_time,"
        "search_start_date,search_window_start_time,search_window_end_time,"
        "preferred_court_name_1,preferred_court_name_2,preferred_court_name_3,"
        "status,is_active,attempt_count,last_run_at,last_error"
    )

    for attempt in range(1, max_retries + 1):
        try:
            q = supabase.from_("court_booking_requests").select(select_cols)
            q = q.eq("is_active", True)
            q = q.in_("status", ["PENDING", "SEARCHING", "CREATED", "QUEUED"])
            q = q.gte("target_date", date.today().isoformat())
            q = q.lte("search_start_date", date.today().isoformat())
            q = q.order("target_date", desc=False)  # ascendente
            q = q.limit(limit)

            res = q.execute()
            data = getattr(res, "data", None)
            if data is None:
                raise RuntimeError(f"Supabase devolvió respuesta vacía (intento {attempt}): {res}")
            return data

        except Exception as e:
            msg = str(e)
            transient = (
                "Internal Server Error" in msg
                or "502" in msg or "503" in msg or "504" in msg
            )
            if attempt < max_retries and transient:
                backoff = 0.8 * attempt
                print(f"[Supabase] 5xx/transient (intento {attempt}), reintentando en {backoff:.1f}s…")
                time.sleep(backoff)
                continue
            raise RuntimeError(f"Error al leer court_booking_requests: {msg}") from e


def update_request_seen(
    request_id: str,
    new_status: str | None = None,
    last_error: str | None = None,
) -> Dict[str, Any]:
    """
    Actualiza una request marcando que el bot la ha revisado:
    - last_run_at = ahora (UTC)
    - attempt_count = attempt_count + 1
    - status = 'SEARCHING' (por defecto) o lo que pases en new_status
    - last_error opcional, para guardar mensaje del radar Better
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    # Empieza con los campos que siempre actualizas
    fields: dict = {
        "last_run_at": now_iso,
    }
    
    # 1) Obtener la fila actual para saber attempt_count
    get_url = f"{REST_URL}/court_booking_requests"
    get_params = {"id": f"eq.{request_id}"}

    get_resp = requests.get(get_url, headers=HEADERS, params=get_params, timeout=30)
    if not get_resp.ok:
        raise RuntimeError(
            f"Error al leer la request {request_id}: "
            f"{get_resp.status_code} {get_resp.text}"
        )

    rows = get_resp.json()
    if not rows:
        raise RuntimeError(f"No se encontró court_booking_request con id={request_id}")

    current = rows[0]
    current_attempts = current.get("attempt_count") or 0

    payload = {
        "last_run_at": datetime.now(timezone.utc).isoformat(),
        "attempt_count": current_attempts + 1,
    }
    if new_status is not None:
        payload["status"] = new_status
    if last_error is not None:
        # guardamos el mensaje del radar / error
        payload["last_error"] = last_error

    patch_url = f"{REST_URL}/court_booking_requests"
    patch_params = {"id": f"eq.{request_id}"}

    patch_headers = {
        **HEADERS,
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    patch_resp = requests.patch(
        patch_url,
        headers=patch_headers,
        params=patch_params,
        json=payload,
        timeout=30,
    )

    if not patch_resp.ok:
        raise RuntimeError(
            f"Error al actualizar la request {request_id}: "
            f"{patch_resp.status_code} {patch_resp.text}"
        )

    updated_rows = patch_resp.json()
    if not updated_rows:
        raise RuntimeError(f"No se devolvieron filas actualizadas para id={request_id}")

    return updated_rows[0]

def update_request_booked(
    request_id: str,
    booked_court_name: str,
    booked_slot_start: str,  # 'HH:MM:SS'
    booked_slot_end: str,    # 'HH:MM:SS'
    last_error: Optional[str] = None,
) -> Dict[str, Any]:
    """Marca la request como BOOKED y guarda los campos booked_*."""
    patch_url = f"{REST_URL}/court_booking_requests"
    patch_params = {"id": f"eq.{request_id}"}
    payload = {
        "status": "BOOKED",
        "is_active": False,
        "booked_court_name": booked_court_name,
        "booked_slot_start": booked_slot_start,
        "booked_slot_end": booked_slot_end,
        "last_run_at": datetime.now(timezone.utc).isoformat(),
    }
    if last_error is not None:
        payload["last_error"] = last_error

    patch_headers = {**HEADERS, "Content-Type": "application/json", "Prefer": "return=representation"}
    resp = requests.patch(patch_url, headers=patch_headers, params=patch_params, json=payload, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Error al actualizar BOOKED {request_id}: {resp.status_code} {resp.text}")
    rows = resp.json()
    if not rows:
        raise RuntimeError(f"PATCH BOOKED sin filas devueltas para id={request_id}")
    return rows[0]

def resolve_credentials_for_request(req: Dict[str, Any]) -> Tuple[str, str]:
    """
    Resuelve usuario/clave leyendo env keys desde booking_accounts (no desde la request).
    Requiere que la fila apunte a better_account_id correcto.
    """
    ba = get_booking_account(req["better_account_id"])
    user_key = ba.get("env_username_key")
    pass_key = ba.get("env_password_key")
    if not user_key or not pass_key:
        raise RuntimeError(f"Faltan env keys en booking_accounts para {req['better_account_id']}")
    username = os.environ.get(user_key)
    password = os.environ.get(pass_key)
    if not username or not password:
        raise RuntimeError(f"Faltan secrets en GitHub Actions: {user_key} / {pass_key}")
    return username, password

def get_booking_account(better_account_id: str) -> Dict[str, Any]:
    """Devuelve la fila de booking_accounts (por id)."""
    url = f"{REST_URL}/booking_accounts"
    params = {"id": f"eq.{better_account_id}", "limit": "1"}
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"booking_accounts GET error: {resp.status_code} {resp.text}")
    rows = resp.json()
    if not rows:
        raise RuntimeError(f"booking_account no encontrado: {better_account_id}")
    return rows[0]

if __name__ == "__main__":
    # Test rápido desde la repo de better
    print("[Supabase Python] Leyendo court_booking_requests PENDING/SEARCHING...")
    pending = get_pending_requests(limit=10)
    print(f"Encontradas {len(pending)} requests.")
    for req in pending:
        print("---")
        print("id:", req["id"])
        print("profile_id:", req["profile_id"])
        print("better_account_id:", req["better_account_id"])
        print("target_date:", req["target_date"])
        print("target_time:", f"{req['target_start_time']}–{req['target_end_time']}")
        print("status:", req["status"])
        print("attempt_count:", req["attempt_count"])
        print("last_run_at:", req["last_run_at"])

    if pending:
        first = pending[0]
        print("\nActualizando la primera request a SEARCHING...")
        updated = update_request_seen(first["id"], new_status="SEARCHING")
        print("Fila actualizada:")
        print(updated)
    else:
        print("No hay requests PENDING/SEARCHING activas.")
