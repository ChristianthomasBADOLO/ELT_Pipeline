# %%
import requests
import pandas as pd
from bs4 import BeautifulSoup
from config_file import headers

def get_amazon_data_books(num_books, ti):
    base_url = f"https://www.amazon.com/s?k=data+engineering+books"
    books = []
    seen_titles = set()
    page = 1

    while len(books) < num_books:
        url = f"{base_url}&page={page}"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            book_containers = soup.find_all("div", {"class": "s-result-item"})
            
            for book in book_containers:
                title = book.find("span", {"class": "a-text-normal"})
                author = book.find("a", {"class": "a-size-base"})
                price = book.find("span", {"class": "a-price-whole"})
                rating = book.find("span", {"class": "a-icon-alt"})
                
                if title and author and price and rating:
                    book_title = title.text.strip()
                    
                    if book_title not in seen_titles:
                        seen_titles.add(book_title)
                        books.append({
                            "Title": book_title,
                            "Author": author.text.strip(),
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip(),
                        })
            
            page += 1
        else:
            print("Failed to retrieve the page")
            break

    books = books[:num_books]
    df = pd.DataFrame(books)
    df = pd.DataFrame(books)
    
    df.drop_duplicates(subset="Title", inplace=True)
    df.to_csv('books.csv', index=False)
    
    
    
    ti.xcom_push(key='book_data', value=df.to_dict('records'))

# # %%
# get_amazon_data_books(5)
# # %%
