import random
import string

def generate_word():
    return ''.join(random.choices(string.ascii_lowercase, k=random.randint(3, 8)))

def generate_text(file_path, size_gb=3):
    words = [generate_word() for _ in range(50_000)]  # 50 000 случайных слов
    target_size = size_gb * 1024 * 1024 * 1024  # Цель — 3 ГБ
    checkpoint = 50 * 1024 * 1024  # Прогресс-бар каждые 50 МБ

    with open(file_path, 'w') as f:
        while f.tell() < target_size:
            sentence = ' '.join(random.choices(words, k=500)) + '\n'
            f.write(sentence)

            # Прогресс-бар
            if f.tell() // checkpoint > (f.tell() - len(sentence)) // checkpoint:
                print(f"✅ Прогресс: {f.tell() / (1024 * 1024):.2f} МБ из {target_size / (1024 * 1024):.2f} МБ")

    print(f"\n🎯 Файл '{file_path}' успешно создан (~{size_gb} ГБ)")

generate_text('large_text_data.txt', size_gb=3)
