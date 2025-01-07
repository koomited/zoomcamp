import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from collections import OrderedDict

def get_books(topic_list):
    # Generate full URLs from the argument provided by user
    all_urls = []
    for topic in topic_list:
        all_urls.append('http://books.toscrape.com/catalogue/category/books/{}/index.html'.format(topic))

    # Instantiate an empty list for holding the dictionary objects
    all_books = []

    # Inform the user that the scraping has started, and when it started
    start_time = datetime.datetime.now()
    print('Book Scraping in Progress... Time Started: {}'.format(datetime.datetime.strftime(start_time, '%d.%m.%Y %H:%M:%S')))

    # Iterate over every URL
    for url in all_urls:
        # Fetch HTML from it
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'lxml')

        # Topic can be extracted from the URL itself
        # I've also removed everything that isn't necessary - '_2' from 'travel_2' for example
        curr_topic = url.split('/')[-2].split('_')[0]

        # article tag is your starting point, as discussed earlier
        books = soup.find_all('article', attrs={'class': 'product_pod'})

        # For every article tag on the webpage
        for book in books:
            # Initialize the variables so the error isn't thrown if data isn't found
            book_title = ''
            book_link = ''
            thumbnail_link = ''
            rating = ''
            price = ''
            availability = ''

            # Check if title exists - if does, update book_title
            if book.find('h3').find('a') != None:
                book_title = book.find('h3').find('a').get('title')

            # Check if link exists - if does, update book_link and thumbnail_link
            if book.find('div', attrs={'class': 'image_container'}).find('a') != None:
                base_book_url = 'http://books.toscrape.com/catalogue/'
                book_url = book.find('div', attrs={'class': 'image_container'}).find('a').get('href')
                book_link = base_book_url + book_url.split('../')[-1]

                base_thumbnail_url = 'http://books.toscrape.com/'
                thumbnail_url = book.find('div', attrs={'class': 'image_container'}).find('img').get('src')
                thumbnail_link = base_thumbnail_url + thumbnail_url.split('../')[-1]

            # Check if rating exists - if does, update rating
            if book.find('p', attrs={'class': 'star-rating'}) != None:
                rating = book.find('p', attrs={'class': 'star-rating'}).get('class')[-1]

            # Check if price and availability exists - if does, update them
            if book.find('div', attrs={'class': 'product_price'}) != None:
                price = book.find('div', attrs={'class': 'product_price'}).find('p', attrs={'class': 'price_color'}).get_text()
                availability = book.find('div', attrs={'class': 'product_price'}).find('p', attrs={'class': 'instock availability'}).get_text().strip()

            # Append the list with Ordered Dictionary from those variables
            all_books.append(OrderedDict({
                'Topic'        : curr_topic,
                'Title'        : book_title,
                'Rating'       : rating,
                'Price'        : price,
                'Availability' : availability,
                'Link'         : book_link,
                'Thumbnail'    : thumbnail_link
            }))

    # Inform the user that scraping has finished and report how long it took
    end_time = datetime.datetime.now()
    duration = int((end_time - start_time).total_seconds())
    print('Scraping Finished!')
    print('\tIt took {} seconds to scrape {} books'.format(duration, len(all_books)))

    # Return Pandas DataFrame representation of the list
    return pd.DataFrame(all_books)

def main():
    # List of topics from which the books will be scraped
    topics = ['travel_2', 'mystery_3', 'historical-fiction_4', 'classics_6', 'philosophy_7']
    # Scrape and return a DataFrame
    books = get_books(topic_list=topics)
    # Save as CSV
    books.to_csv('books.csv', index=False)

if __name__ == '__main__':
    main()