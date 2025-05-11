import re
import time
import psutil
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm  # Для прогресс-бара

# Путь к файлу
data_path = "large_text_data.txt"

# Функция для обработки части текста
def process_chunk(chunk):
    words = re.findall(r'\w+', chunk.lower())
    return Counter(words)

if __name__ == '__main__':
    # Замер стартового состояния системы
    start_time = time.time()
    start_cpu = psutil.cpu_percent()
    start_mem = psutil.virtual_memory().percent

    # Чтение данных по блокам (streaming) и сбор статистики
    word_counts = Counter()

    # Размер блока данных (чтобы не читать весь файл сразу)
    chunk_size = 1024 * 1024  # 1 MB блоки

    # Подсчёт общего размера файла для корректного отображения прогресса
    total_size = 0
    with open(data_path, "r", encoding="utf-8") as file:
        file.seek(0, 2)  # Переход в конец файла
        total_size = file.tell()

    with open(data_path, "r", encoding="utf-8") as file:
        with ProcessPoolExecutor() as executor:
            # Прогресс-бар на основе размеров файла
            with tqdm(total=total_size, unit='B', unit_scale=True, desc="Processing File") as pbar:
                while True:
                    chunk = file.read(chunk_size)
                    if not chunk:
                        break
                    word_counts.update(process_chunk(chunk))
                    pbar.update(len(chunk))

    # Замер финального состояния системы
    end_time = time.time()
    end_cpu = psutil.cpu_percent()
    end_mem = psutil.virtual_memory().percent

    # Вывод топ-20 слов
    print("\n📊 Top 20 Words:")
    for word, count in word_counts.most_common(20):
        print(f"{word}: {count}")

    # Вывод производительности
    print("\n🚀 Execution Time (Python): {:.4f} seconds".format(end_time - start_time))
    print(f"🔥 CPU Usage (Python): {end_cpu}%")
    print(f"💾 RAM Usage (Python): {end_mem}%")
    print(f"🧠 Total CPU Cores Used: {psutil.cpu_count(logical=True)}")
