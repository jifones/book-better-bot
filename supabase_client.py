import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

# Cargar .env
load_dotenv()

# Usamos exactamente las env que tienes:
# VITE_SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY
SUPABASE_URL = os.environ.get("VITE_SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

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


def get_pending_requests(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Lee las court_booking_requests con status PENDING/SEARCHING y is_active = true.
    Devuelve una lista de diccionarios con las filas.
    """
    params = {
        "status": "in.(PENDING,SEARCHING)",
        "is_active": "eq.true",
        "order": "created_at.asc",
        "limit": str(limit),
    }

    url = f"{REST_URL}/court_booking_requests"
    response = requests.get(url, headers=HEADERS, params=params, timeout=30)

    if not response.ok:
        raise RuntimeError(
            f"Error al leer court_booking_requests: "
            f"{response.status_code} {response.text}"
        )

    return response.json()


def update_request_seen(request_id: str, new_status: Optional[str] = None) -> Dict[str, Any]:
    """
    Actualiza una request marcando que el bot la ha revisado:
    - last_run_at = ahora (UTC)
    - attempt_count = attempt_count + 1
    - status = 'SEARCHING' (por defecto) o lo que pases en new_status

    Devuelve la fila actualizada.
    """
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
