# book_test.py
import os
import logging
from datetime import date, time
from book_better.main import book_with_credit_for_date

# 🔇 Bajar el ruido de logging: solo WARNING y errores
logging.getLogger().setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

from dotenv import load_dotenv

from book_better.main import book_with_credit_for_date
from book_better.utils import parse_time

# 👇 Ajusta esta fecha y horas según lo que quieras probar
TARGET_DATE = date(2025, 12, 4)   # 3 de diciembre
TARGET_START = "2000"  # "hhmm"
TARGET_END = "2100"    # "hhmm"

if __name__ == "__main__":
    # Convertimos los strings "1900" / "2000" a datetime.time
    start_time = parse_time(TARGET_START)
    end_time = parse_time(TARGET_END)

    print(
        f">>> Intentando reservar con crédito el {TARGET_DATE} "
        f"de {start_time.strftime('%H:%M')}–{end_time.strftime('%H:%M')} "
        f"para el usuario 'javier'..."
    )

    result = book_with_credit_for_date(
        target_date=TARGET_DATE,
        start_time=start_time,
        end_time=end_time,
        better_account="javier",
    )

    print(">>> Resultado:", result)
