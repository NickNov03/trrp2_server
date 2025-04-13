import json
import subprocess

def run_js_analysis(orders):
    orders_json = json.dumps(orders)
    result = subprocess.run(
        ['node', r'analysis.js', orders_json],  # Запускаем скрипт Node.js
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print("Анализ завершен, файл сохранен:", result.stdout.strip())
    else:
        print("Ошибка выполнения:", result.stderr)