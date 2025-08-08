import requests
import time
import csv
import random
import concurrent.futures
from bs4 import BeautifulSoup
from threading import Lock

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
}

MAX_THREADS = 10
csv_lock = Lock()

def extract_movie_details(movie_link):
    try:
        time.sleep(random.uniform(0.1, 0.3))
        response = requests.get(movie_link, headers=headers)
        movie_soup = BeautifulSoup(response.content, 'html.parser')

        title, date, rating, plot_text = None, None, None, None

        section = movie_soup.find('section', attrs={'class': 'ipc-page-section'})
        if section:
            divs = section.find_all('div', recursive=False)
            if len(divs) > 1:
                target_div = divs[1]
                title_tag = target_div.find('h1')
                if title_tag:
                    span = title_tag.find('span')
                    title = span.get_text() if span else None

                date_tag = target_div.find('a', href=lambda href: href and 'releaseinfo' in href)
                if date_tag:
                    date = date_tag.get_text().strip()

        rating_tag = movie_soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
        if rating_tag:
            rating = rating_tag.get_text().strip()

        plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
        if plot_tag:
            plot_text = plot_tag.get_text().strip()

        if all([title, date, rating, plot_text]):
            with csv_lock:
                with open('movies.csv', mode='a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    writer.writerow([title, date, rating, plot_text])
                    print(f"✔ {title}")

    except Exception as e:
        print(f"Erro ao processar {movie_link}: {e}")

def extract_movies(soup):
    movie_list = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
    movie_items = movie_list.find_all('li')
    movie_links = ['https://imdb.com' + item.find('a')['href'].split('?')[0] for item in movie_items]

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)

def main():
    start_time = time.time()

    with open('movies.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Title', 'Release Date', 'Rating', 'Synopsis'])

    print("🔍 Coletando filmes populares da IMDb...")
    url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    extract_movies(soup)

    end_time = time.time()
    print(f"⏱ Concluído em {end_time - start_time:.2f} segundos.")

if __name__ == '__main__':
    main()
