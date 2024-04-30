import json
import requests
import streamlit as st
from datetime import datetime
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from newsapi import NewsApiClient

# Initialize News API client
newsapi = NewsApiClient(api_key="b056a24e5faf468ca4f115f81929da23")

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Function to extract date from datetime string
def extract_date(datetime_str):
    published_datetime = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')
    published_date = published_datetime.strftime('%Y-%m-%d')
    return published_date

# Function to fetch and publish news to Kafka
def fetch_and_publish_news(keyword, limit=10):
    sources = 'bbc-news,cnn,fox-news,nbc-news,the-guardian-uk,the-new-york-times,the-washington-post,usa-today,independent,daily-mail'
    all_articles = newsapi.get_everything(q=keyword,
                                          sources=sources,
                                          language='en',
                                          page_size=limit)
    
    # Publish each article to Kafka
    for article in all_articles['articles']:
        # Fetch full content from the article URL using web scraping
        article_data = {
            'keyword': keyword,
            'title': article['title'],
            'description': article['description'],
            'author': article['author'],
            'link': article['url'],
            'image': article['urlToImage'],
            'publish_date': extract_date(article['publishedAt']),
            'event_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send('news_topic', json.dumps(article_data).encode('utf-8'))

# Function to fetch full content of an article from its URL
def fetch_full_content(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        full_content = ""
        # Find and concatenate all paragraphs of the article
        for paragraph in soup.find_all('p'):
            full_content += paragraph.get_text() + "\n\n"
        return full_content
    except Exception as e:
        print("Error fetching full content:", e)
        return ""

# Streamlit app
def main():
    st.title('News App')
    keyword = st.text_input('Enter a keyword:')
    if st.button('Search'):
        if keyword:
            fetch_and_publish_news(keyword, limit=10)
            st.success('10 search results found')

            # Display fetched news articles with full content
            st.subheader('Fetched News Articles:')
            sources = 'bbc-news,cnn,fox-news,nbc-news,the-guardian-uk,the-new-york-times,the-washington-post,usa-today,independent,daily-mail'
            all_articles = newsapi.get_everything(q=keyword,
                                                  sources=sources,
                                                  language='en',
                                                  page_size=10)
            for article in all_articles['articles']:
                if article['urlToImage']:
                    st.write(f"**Title:** {article['title']}")
                    st.write(f"**Description:** {article['description']}")
                    st.write(f"**Author:** {article['author']}")
                    st.write(f"**Publish Date:** {extract_date(article['publishedAt'])}")
                    st.image(article['urlToImage'], caption='Image', use_column_width=True)
                    st.write('---')
        else:
            st.warning('Please enter a keyword.')
        
if __name__ == "__main__":
    main()
