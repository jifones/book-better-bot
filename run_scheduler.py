import sys
from datetime import datetime, date, time

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


def main() -> int:
    now = datetime.now()  # Por ahora usamos la hora del sistema (tu PC o GitHub runner)
    print(f"[Scheduler] Ejecutando a las {now.isoformat()}")

    # 1) Leer las requests pendientes/SEARCHING
    try:
        requests = get_pending_requests(limit=50)
    except Exception as e:
        print(f"[Scheduler] Error leyendo requests: {e}", file=sys.stderr)
        return 1

    print(f"[Scheduler] Encontradas {len(requests)} requests PENDING/SEARCHING activas.")

    for req in requests:
        rid = req["id"]
        action = should_process_request(req, now)

        if action == "EXPIRE":
            print(f"[Scheduler] Marcando como EXPIRED request {rid} (target_date ya pasó).")
            try:
                update_request_seen(rid, new_status="EXPIRED")
            except Exception as e:
                print(f"[Scheduler] Error al marcar EXPIRED {rid}: {e}", file=sys.stderr)
            continue

        if action == "SKIP":
            print(f"[Scheduler] Saltando request {rid} (todavía no toca o fuera de ventana horaria).")
            continue

        if action == "PROCESS":
            print(f"[Scheduler] >>> Toca procesar request {rid} ahora mismo.")
            # De momento, SOLO marcamos que ha sido revisada y la dejamos en SEARCHING.
            try:
                updated = update_request_seen(rid, new_status="SEARCHING")
                print(f"[Scheduler] Request {rid} actualizada (attempt_count={updated['attempt_count']}, last_run_at={updated['last_run_at']}).")
            except Exception as e:
                print(f"[Scheduler] Error al actualizar {rid}: {e}", file=sys.stderr)
                continue

            # Aquí, más adelante, es donde meteremos la lógica de Better:
            # - consultar slots
            # - reservar
            # - actualizar a BOOKED / FAILED

    print("[Scheduler] Fin de la ejecución.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
