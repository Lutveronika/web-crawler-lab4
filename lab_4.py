import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import re
from collections import Counter
from threading import Thread
from queue import Queue
import threading
import time

def download_page(url, retries=2):
    for attempt in range(retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            }
            response = requests.get(url, timeout=15, headers=headers)
            if response.status_code == 200:
                return response.text
            else:
                print(f"  Ошибка {response.status_code}: {url}")
                return None
        except Exception as e:
            print(f"  Попытка {attempt+1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2)
    return None

def extract_links(html, current_url, domain):
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        full_url = urljoin(current_url, href)
        parsed = urlparse(full_url)
        if parsed.netloc == domain and parsed.scheme in ('http', 'https'):
            links.append(full_url)
    return list(set(links))

def clean_text(html):
    soup = BeautifulSoup(html, 'html.parser')
    for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
        element.decompose()
    text = soup.get_text(separator=' ', strip=True)
    text = text.lower()
    text = re.sub(r'\s+', ' ', text)
    return text

def count_words(text):
    words = re.findall(r'[а-яё]{3,}', text)
    stop_words = {'и', 'в', 'на', 'с', 'к', 'у', 'о', 'об', 'от', 'до', 'по', 'за',
                  'из', 'без', 'под', 'над', 'для', 'при', 'через', 'или', 'а', 'но',
                  'да', 'не', 'ни', 'что', 'как', 'так', 'все', 'это', 'был', 'его',
                  'ее', 'их', 'ты', 'мы', 'вы', 'он', 'она', 'оно', 'они', 'этот',
                  'эта', 'эти', 'бы', 'же', 'ли', 'ведь', 'вот', 'во', 'между',
                  'после', 'перед', 'без', 'вне', 'внутри', 'год', 'лет', 'году'}
    filtered_words = [word for word in words if word not in stop_words]
    return filtered_words

def crawl(start_url, max_depth=2, num_threads=2):
    domain = urlparse(start_url).netloc
    print(f"\nНачинаем обход сайта: {domain}")
    print(f"Максимальная глубина: {max_depth}")
    print(f"Количество потоков: {num_threads}\n")
    
    task_queue = Queue()
    task_queue.put((start_url, 0))
    
    visited = set()
    visited_lock = threading.Lock()
    
    all_words = []
    words_lock = threading.Lock()
    
    pages_processed = 0
    pages_lock = threading.Lock()
    
    def worker():
        nonlocal pages_processed
        
        while True:
            try:
                url, depth = task_queue.get(timeout=3)
            except:
                break
            
            with visited_lock:
                if url in visited:
                    task_queue.task_done()
                    continue
                visited.add(url)
            
            with pages_lock:
                pages_processed += 1
                current_page = pages_processed
            
            print(f"[{current_page}] Глубина {depth}: {url}")
            
            html = download_page(url)
            if html:
                text = clean_text(html)
                words = count_words(text)
                
                with words_lock:
                    all_words.extend(words)
                
                print(f"   Найдено слов: {len(words)}")
                
                if depth < max_depth:
                    links = extract_links(html, url, domain)
                    print(f"   Найдено ссылок: {len(links)}")
                    
                    with visited_lock:
                        for link in links:
                            if link not in visited:
                                task_queue.put((link, depth + 1))
            
            print(f"   Осталось в очереди: {task_queue.qsize()}\n")
            task_queue.task_done()
            
            time.sleep(0.3)
    
    threads = []
    for _ in range(num_threads):
        t = Thread(target=worker)
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()
    
    print(f"\nОбход закончен!")
    print(f"Обработано страниц: {pages_processed}")
    print(f"Всего собрано слов: {len(all_words)}")
    
    return all_words, visited

def get_top_words(words, top_n=10):
    counter = Counter(words)
    return counter.most_common(top_n)

def save_results(start_url, max_depth, pages_processed, top_words):
    results = {
        "сайт": start_url,
        "глубина_обхода": max_depth,
        "обработано_страниц": pages_processed,
        "топ_10_слов": []
    }
    
    for word, count in top_words:
        results["топ_10_слов"].append({
            "слово": word,
            "количество": count
        })
    
    with open("results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print("\nРезультаты сохранены в файл results.json")

def main():
    START_URL = "https://www.vedomosti.ru/"
    MAX_DEPTH = 2
    NUM_THREADS = 2
    
    print("=" * 60)
    print("ВЕБ-КРАУЛЕР - Лабораторная работа №4")
    print("Вариант 4: Ведомости - топ-10 частых слов")
    print("Многопоточная версия")
    print("=" * 60)
    
    all_words, visited = crawl(START_URL, MAX_DEPTH, NUM_THREADS)
    top_words = get_top_words(all_words, 10)
    
    print("\n" + "=" * 60)
    print("ТОП-10 САМЫХ ЧАСТЫХ СЛОВ")
    print("=" * 60)
    
    for i, (word, count) in enumerate(top_words, 1):
        print(f"{i:2}. {word:15} -> {count:5} раз")
    
    print("=" * 60)
    
    unique_words = len(set(all_words))
    print(f"Уникальных слов: {unique_words}")
    
    save_results(START_URL, MAX_DEPTH, len(visited), top_words)
    print("\nРабота завершена!")

if __name__ == "__main__":
    main()