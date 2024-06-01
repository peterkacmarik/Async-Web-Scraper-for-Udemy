import asyncio
from scraper.fetch_pages import main_script
from dotenv import load_dotenv
import os
from logs import logger
import logging


# Load environment variables
load_dotenv()

# Load logger settings from .env file
LOG_DIR_MAIN = os.getenv('LOG_DIR_MAIN')

# Create logger object
logger = logger.get_logger(log_file=LOG_DIR_MAIN, log_level=logging.INFO)


if __name__ == "__main__":
    # Start and end pages to fetch from the website
    start_page: int = 1
    end_page: int = 1
    
    # Expression to search 
    expression = "ChatGPT"
    
    # Condition when editing the search term
    if " " in expression:
        # Replace whitespace with "+" in the search term
        wanted_expression = expression.replace(" ", "+")

        # List of URLs to fetch from the website
        urls: list = [
            f"https://www.udemy.com/courses/search/?kw={wanted_expression}&p={page}&q={wanted_expression}&src=sac"
            for page in range(start_page, end_page + 1)
            ]
    else:
        # List of URLs to fetch from the website
        urls: list = [
            f"https://www.udemy.com/courses/search/?p={page}&q={expression}&src=ukw"
            for page in range(start_page, end_page + 1)
        ]

    # Max number of concurrent tasks to run
    max_concurrent_tasks: int = 4
    
    # Timeout in milliseconds before timing out
    timeout: int = 30000

    # Run the main function
    asyncio.run(main_script(urls, timeout, max_concurrent_tasks))
