import asyncio
from datetime import datetime
import logging
import os
import pandas as pd
from playwright.async_api import async_playwright
from scraper.parse_data import (
    start_parse_course_links,
    start_parsing_detail_links,
    start_parsing_instructor_links,
)
from scraper.checker import CheckRecords
from database.models import (
    DatabaseManagerSettings,
    CourseLinks,
    DetailCourseLinks,
    InstructorDetails,
    MergeData,
)
from dotenv import load_dotenv
from logs import logger


# Load environment variables
load_dotenv()

# Load logger settings from .env file
LOG_DIR_FETCH_PAGES = os.getenv('LOG_DIR_FETCH_PAGES')

# Create logger object
logger = logger.get_logger(log_file=LOG_DIR_FETCH_PAGES, log_level=logging.INFO)


async def fetch_course_links(index: int, url: str, timeout=30000):
    """
    Asynchronously fetches the HTML content of a given URL using Playwright.

    Args:
        index (int): The index of the URL in the list of URLs to fetch.
        url (str): The URL to fetch.
        timeout (int, optional): The timeout in milliseconds for the page load. Defaults to 30000.

    Returns:
        str or None: The HTML content of the fetched page if successful, None otherwise.
    """
    async with async_playwright() as p:
        # Launch a Chromium browser
        browser = await p.chromium.launch(headless=False)
        
        # Create a new page
        page = await browser.new_page()
        
        try:
            # Navigate to Udemy search page
            await page.goto(url, timeout=timeout)
            
            # Wait for the page to finish loading the content
            await page.wait_for_load_state("networkidle", timeout=timeout)
            
            # Get the HTML content of the page
            html_content = await page.content()
            
            logger.info(f"Index: {index + 1} | Page fetched: {url}")
            
            # Return HTML content
            return html_content
        
        # Catch any exceptions and print the error message
        except Exception as e:
            logger.info(f"An error occurred: {e} | URL: {url}")
            return None
        finally:
            # Close the browser and page 
            await browser.close()


async def bounded_fetch(semaphore: asyncio.Semaphore, index: int, course_link: str, timeout: int):
    """
    Asynchronously fetches the HTML content of a given course link using a semaphore to limit the number of concurrent requests.

    Args:
        semaphore (asyncio.Semaphore): The semaphore to limit the number of concurrent requests.
        index (int): The index of the course link in the list of course links to fetch.
        course_link (str): The URL of the course link to fetch.
        timeout (int, optional): The maximum time in milliseconds to wait for the request to complete. Defaults to 30000.

    Returns:
        str: The HTML content of the fetched course link.
    """
    # logger.info(f"Index: {index + 1} | Course link: {course_link}")
    async with semaphore:
        return await fetch_course_links(index, course_link, timeout)


async def start_analysis_page_links(urls: list, timeout: int, max_concurrent_tasks: int):
    """
    Asynchronously fetches the HTML content of a list of page links using a semaphore to limit the number of concurrent requests.

    Args:
        urls (list): A list of URLs of the page links to fetch.
        timeout (int): The maximum time in milliseconds to wait for the request to complete.
        max_concurrent_tasks (int): The maximum number of concurrent requests to make.

    Returns:
        list: A list of the HTML content of the fetched page links.
    """
    # Semaphore to limit the number of concurrent requests
    semaphore_page_links: asyncio = asyncio.Semaphore(max_concurrent_tasks)
    tasks_html: list = []
    
    start_time_fetch_page: datetime = datetime.now()
    
    # Start fetching page links
    for index, page_url in enumerate(urls):
        # Create a task to fetch the HTML content of the page
        task_html: asyncio = asyncio.create_task(bounded_fetch(semaphore_page_links, index, page_url, timeout=timeout))
        
        # Add the task to the list of tasks
        tasks_html.append(task_html)
    # Wait for all tasks to complete 
    html_content: list = await asyncio.gather(*tasks_html)
    
    end_time_fetch_page: datetime = datetime.now()
    logger.info(f"\t*** Done fetching page links in {end_time_fetch_page - start_time_fetch_page} ***")
    
    # Return HTML content
    return html_content


async def start_analysis_detail_links(df_course_links: pd.DataFrame, timeout: int, max_concurrent_tasks: int):
    """
    Asynchronously fetches the detail links from a DataFrame of course links.

    Args:
        df_course_links (pandas.DataFrame): The DataFrame containing the course links.
        timeout (int): The timeout value for the HTTP requests.
        max_concurrent_tasks (int): The maximum number of concurrent tasks allowed.

    Returns:
        list: A list of the HTML contents of the detail links.

    Raises:
        None

    Description:
        This function starts the analysis of the detail links by creating a semaphore to limit the number of concurrent tasks.
        It then creates a list of tasks to fetch the detail links using the `bounded_fetch` function.
        The tasks are executed concurrently using `asyncio.gather` and the results are stored in a list.
        The function logs the time taken to fetch the details and returns the list of detail HTML contents.
    """
    # Semaphore to limit the number of concurrent tasks
    semaphore_detail_course_links: asyncio = asyncio.Semaphore(max_concurrent_tasks)
    tasks_details: list = []
    
    start_time_bounded_fetch: datetime = datetime.now()
    
    # Start fetching detail links
    for index, course_link in enumerate(df_course_links["url"]):
        # Create a task to fetch the HTML content of the detail link
        task_detail: asyncio = asyncio.create_task(bounded_fetch(semaphore_detail_course_links, index, course_link, timeout=timeout))
        
        # Add the task to the list of tasks
        tasks_details.append(task_detail)
    
    # Wait for all tasks to complete
    detail_html_contents: list = await asyncio.gather(*tasks_details, return_exceptions=True)
    
    end_time_bounded_fetch: datetime = datetime.now()
    logger.info(f"\t*** Done fetching details in {end_time_bounded_fetch - start_time_bounded_fetch} ***")
    
    # Return HTML content
    return detail_html_contents


async def start_analysis_instructor_links(df_detail_course_links: pd.DataFrame, timeout: int, max_concurrent_tasks: int):
    """
    Asynchronously fetches the HTML contents of instructor links from a DataFrame of detail course links.

    Args:
        df_detail_course_links (pandas.DataFrame): The DataFrame containing the detail course links.
        timeout (int): The timeout value for the HTTP requests.
        max_concurrent_tasks (int): The maximum number of concurrent tasks allowed.

    Returns:
        list: A list of the HTML contents of the instructor links.

    Raises:
        None

    Description:
        This function starts the analysis of the instructor links by creating a semaphore to limit the number of concurrent tasks.
        It then creates a list of tasks to fetch the instructor links using the `bounded_fetch` function.
        The tasks are executed concurrently using `asyncio.gather` and the results are stored in a list.
        The function logs the time taken to fetch the instructor links and returns the list of instructor HTML contents.
    """
    # Semaphore to limit the number of concurrent tasks
    semafore_instructor_links: asyncio = asyncio.Semaphore(max_concurrent_tasks)
    
    instructor_urls: list = df_detail_course_links["instructor_url"]
    task_detail_links: list = []
    
    start_time_fetch_instructor: datetime = datetime.now()
    
    # Start fetching instructor links
    for index, instructor_url in enumerate(instructor_urls):
        # Create a task to fetch the HTML content of the detail link
        task_instructor: asyncio = asyncio.create_task(bounded_fetch(semafore_instructor_links, index, instructor_url, timeout=timeout))
        
        # Add the task to the list of tasks
        task_detail_links.append(task_instructor)
    
    # Wait for all tasks to complete
    detail_html_contents_instructor: list = await asyncio.gather(*task_detail_links)
    
    end_time_fetch_instructor: datetime = datetime.now()
    logger.info(f"\t*** Done fetching instructor links in {end_time_fetch_instructor - start_time_fetch_instructor} ***")
    
    # Return HTML content
    return detail_html_contents_instructor


async def main_script(urls: list, timeout: int, max_concurrent_tasks: int):
    """
    Asynchronously scrapes data from a website and parses it into multiple dataframes.
    
    Args:
        urls (list): A list of URLs to scrape.
        timeout (int): The maximum time in seconds to wait for a response from a URL.
        max_concurrent_tasks (int): The maximum number of concurrent tasks to run.
        
    Returns:
        None
        
    Raises:
        None
    """
    # Start main script to fetch and parse data from website
    total_time_start: datetime = datetime.now()
    
    # Create database manager
    db_manager_settings = DatabaseManagerSettings()
    
    # Fetch and parse course links
    logger.info("\n\t*** Start fetching Page links ***")
    html_content = await start_analysis_page_links(urls, timeout, max_concurrent_tasks)
    complet_list_urls, complet_list_expressions = start_parse_course_links(html_content)
    
    # Compare data with database
    df_course_links = CheckRecords().compare_data_db(complet_list_urls, complet_list_expressions)

    # Insert data into database table CourseLinks
    db_manager_settings.insert_data(df_course_links, CourseLinks)
    logger.info("\t*** Data inserted into table CourseLinks ***")

    # Fetch and parse detail links
    logger.info("\n\t*** Start fetching Detail links ***")
    detail_html_contents = await start_analysis_detail_links(df_course_links, timeout, max_concurrent_tasks)
    detail_data = start_parsing_detail_links(detail_html_contents)
    df_detail_course_links = pd.DataFrame(detail_data)

    # Insert detail data from the DataFrame into the database
    db_manager_settings.insert_data(df_detail_course_links, DetailCourseLinks)
    logger.info("\t*** Data inserted into table DetailCourseLinks ***")

    # Fetch and parse instructor links
    logger.info("\n\t*** Start fetching Instructor links ***")
    detail_html_contents_instructor = await start_analysis_instructor_links(df_detail_course_links, timeout, max_concurrent_tasks)
    instructor_detail_data = start_parsing_instructor_links(detail_html_contents_instructor)
    df_instructor_detail = pd.DataFrame(instructor_detail_data)

    # Insert detail data from the DataFrame into the database
    db_manager_settings.insert_data(df_instructor_detail, InstructorDetails)
    logger.info("\n\t*** Data inserted into table InstructorDetails ***")
    
    # Check if all DataFrames have the same number of rows
    if len(df_course_links) == len(df_detail_course_links) == len(df_instructor_detail):
        # Merge all dataframes into one
        df_merged = pd.concat([df_course_links, df_detail_course_links, df_instructor_detail], axis=1)
        
        # Insert merged data into the database
        db_manager_settings.insert_data(df_merged, MergeData)
        logger.info("\n\t*** Data inserted into table CombinedData ***")
    else:
        logger.info("Error: DataFrames do not have the same number of rows.")
        logger.info(f"df_course_links: {len(df_course_links)}, df_detail_course_links: {len(df_detail_course_links)}, df_instructor_detail: {len(df_instructor_detail)}")
    
    # Close the database connection once at the end
    db_manager_settings.close_connection()
    
    # Total time for scraping the website and parsing the data from it
    total_time_end: datetime = datetime.now()
    logger.info(f"Total time for scraping: {total_time_end - total_time_start}")
    
    
    # Date and time
    date_time: str = datetime.now().strftime("%Y-%m-%d_%H-%M")
    # Wanted expression
    wanted_expression = "_".join(complet_list_expressions[0].split(" "))
    
    # Save dataframe to csv
    df_course_links.to_csv(f"async-web-scraper-udemy/dataset/course_links_{wanted_expression}_{date_time}.csv", index=False)
    # Save dataframe to excel
    df_course_links.to_excel(f"async-web-scraper-udemy/dataset/course_links_{wanted_expression}_{date_time}.xlsx", index=False)
    
    # Save the DataFrame to a CSV file
    df_detail_course_links.to_csv(f"async-web-scraper-udemy/dataset/detail_data_{wanted_expression}_{date_time}.csv", index=False, encoding="utf-8",)
    # Save the DataFrame to an Excel file
    df_detail_course_links.to_excel(f"async-web-scraper-udemy/dataset/detail_data_{wanted_expression}_{date_time}.xlsx", index=False)
    
    # Save the DataFrame to a CSV file
    df_instructor_detail.to_csv(f"async-web-scraper-udemy/dataset/instructor_detail_data_{wanted_expression}_{date_time}.csv", index=False, encoding="utf-8",)
    # Save the DataFrame to an Excel file
    df_instructor_detail.to_excel(f"async-web-scraper-udemy/dataset/instructor_detail_data_{wanted_expression}_{date_time}.xlsx", index=False)