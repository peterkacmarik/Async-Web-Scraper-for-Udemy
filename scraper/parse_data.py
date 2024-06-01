from datetime import datetime
import logging
import os
import re
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from logs import logger


# Load environment variables
load_dotenv()

# Load logger settings from .env file
LOG_DIR_PARSE_DATA = os.getenv('LOG_DIR_PARSE_DATA')

# Create logger object
logger = logger.get_logger(log_file=LOG_DIR_PARSE_DATA, log_level=logging.INFO)


def parse_course_links(html_content: str):
    """
    Parses course links and wanted expressions from the given HTML content.

    Args:
        html_content (str): The HTML content to parse.

    Returns:
        tuple: A tuple containing two lists. The first list contains the parsed course links, 
        and the second list contains the parsed wanted expressions.

    Raises:
        None

    Example:
        >>> parse_course_links("<html><body><h3>Course 1</h3><h3>Course 2</h3></body></html>")
        (['https://www.udemy.com/course/1', 'https://www.udemy.com/course/2'], ['Wanted Expression 1', 'Wanted Expression 2'])
    """
    # Create BeautifulSoup object from HTML content
    soup: BeautifulSoup = BeautifulSoup(html_content, "html.parser")

    # Find all h3 elements
    h3_title: list = soup.find_all("h3", {"data-purpose": "course-title-url"})
    
    # Find the h1 element
    wanted_expression: str = soup.find("h1", {"class": "ud-heading-xl search--header-title--dNQ1f"}).text

    # Extract the text from the h1 element
    match: re.Match = re.search(r"“([^”]*)”", wanted_expression)

    wanted_expressions: list = []  # List to store the extracted text
    course_links: list = []  # List to store the course links
    
    # Loop through the h3 elements and extract the text
    for course_link in h3_title:
        # find the link from the h3 element
        link: str = course_link.find("a").get("href").strip()
        
        # check if the link starts with "/course/"
        if link and link.startswith("/course/"):
            # add the link to the course_links list
            full_link: str = f"https://www.udemy.com{link}".strip()
            course_links.append(full_link)

            # extract the text from the h3 element
            extracted_text: str = match.group(1)
            # add the extracted text to the wanted_expressions list
            wanted_expressions.append(extracted_text)
    # Return the parsed course links and wanted expressions
    return course_links, wanted_expressions


def parse_detail_course_links(detail_html_contents: list):
    """
    Parses the detail course links from the given HTML contents and returns a dictionary containing the parsed data.

    Parameters:
    - detail_html_contents (list): A list of HTML contents containing detail course links.

    Returns:
    - detail_data (dict): A dictionary containing the parsed detail data. The dictionary has the following keys:
        - "course_title" (list): A list of course titles.
        - "course_subtitle" (list): A list of course subtitles.
        - "course_num_students" (list): A list of numbers of students enrolled in each course.
        - "course_rating" (list): A list of ratings for each course.
        - "course_created_by" (list): A list of names of the creators of each course.
        - "course_last_updated" (list): A list of timestamps indicating when each course was last updated.
        - "course_url" (list): A list of URLs of each course.
        - "instructor_name" (list): A list of names of the instructors.
        - "instructor_ratings" (list): A list of ratings given by the instructors.
        - "instructor_reviews" (list): A list of reviews given by the instructors.
        - "instructor_students" (list): A list of numbers of students followed by the instructors.
        - "instructor_courses" (list): A list of numbers of courses taught by the instructors.
        - "instructor_url" (list): A list of URLs of the instructors.

    Note:
    - If an HTML content is None, it is skipped and not parsed.
    - If a tag or attribute is not found, a default value is appended to the corresponding list.

    Example:
    >>> detail_html_contents = ["<html><body><h1 class='ud-heading-xl clp-lead__title clp-lead__title--small'>Course Title</h1><div class='ud-text-md clp-lead__headline'>Course Subtitle</div><div class='enrollment'>100 students</div><span class='ud-sr-only'>4.5/5 stars</span><a class='ud-btn ud-btn-large ud-btn-link ud-heading-md ud-text-sm ud-instructor-links'>Creator Name</a><div class='last-update-date'>Last Updated: 2021-01-01</div><meta property='og:url' content='https://www.udemy.com/course/course-url'></meta></body></html>"]
    >>> parse_detail_course_links(detail_html_contents)
    {
        "course_title": ["Course Title"],
        "course_subtitle": ["Course Subtitle"],
        "course_num_students": ["100 students"],
        "course_rating": ["4.5/5 stars"],
        "course_created_by": ["Creator Name"],
        "course_last_updated": ["Last Updated: 2021-01-01"],
        "course_url": ["https://www.udemy.com/course/course-url"],
        "instructor_name": ["N/A"],
        "instructor_ratings": ["N/A"],
        "instructor_reviews": ["N/A"],
        "instructor_students": ["N/A"],
        "instructor_courses": ["N/A"],
        "instructor_url": ["N/A"]
    }
    """
    # Initialize an empty dictionary to store the parsed data
    detail_data = {
        "course_title": [],
        "course_subtitle": [],
        "course_num_students": [],
        "course_rating": [],
        "course_created_by": [],
        "course_last_updated": [],
        "course_url": [],
        "instructor_name": [],
        "instructor_ratings": [],
        "instructor_reviews": [],
        "instructor_students": [],
        "instructor_courses": [],
        "instructor_url": [],
    }
    # Loop through each HTML content in the list
    for html_content in detail_html_contents:
        
        # Check if the HTML content is None
        if html_content is None:
            continue  # Skip None values
        
        # Parse the HTML content using BeautifulSoup
        soup: BeautifulSoup = BeautifulSoup(html_content, "html.parser")

        # Extract the data from the HTML content
        try:
            detail_data["course_title"].append(soup.find("h1", {"class": "ud-heading-xl clp-lead__title clp-lead__title--small"}).text)
        except:
            detail_data["course_title"].append("N/A")
        try:
            detail_data["course_subtitle"].append(soup.find("div", class_="ud-text-md clp-lead__headline").text)
        except:
            detail_data["course_subtitle"].append("N/A")
        try:
            detail_data["course_num_students"].append(soup.find("div", class_="enrollment").text)
        except:
            detail_data["course_num_students"].append("N/A")
        try:    
            detail_data["course_rating"].append(soup.find("span", class_="ud-sr-only").text)
        except:
            detail_data["course_rating"].append("N/A")
        try:
            detail_data["course_created_by"].append(soup.find("a", class_="ud-btn ud-btn-large ud-btn-link ud-heading-md ud-text-sm ud-instructor-links").text)
        except:
            detail_data["course_created_by"].append("N/A")
        try:
            detail_data["course_last_updated"].append(soup.find("div", class_="last-update-date").text)
        except:
            detail_data["course_last_updated"].append("N/A")
        try:
            detail_data["course_url"].append(soup.find("meta", property="og:url").get("content"))
        except:
            detail_data["course_url"].append("N/A")
        try:
            detail_data["instructor_name"].append(soup.find("div", class_="ud-heading-lg ud-link-underline instructor--instructor__title--S9oZ4",).text)
        except:
            detail_data["instructor_name"].append("N/A")
        try:
            detail_data["instructor_ratings"].append(soup.find("div", {"class": "instructor--instructor__image-and-stats--6Nbsa"}).find("ul", class_="ud-unstyled-list ud-block-list").find_all("div", class_="ud-block-list-item-content")[0].text)
        except:
            detail_data["instructor_ratings"].append("N/A")
        try:
            detail_data["instructor_reviews"].append(soup.find("div", {"class": "instructor--instructor__image-and-stats--6Nbsa"}).find_all("div", class_="ud-block-list-item-content")[1].text)
        except:
            detail_data["instructor_reviews"].append("N/A")
        try:
            detail_data["instructor_students"].append(soup.find("div", {"class": "instructor--instructor__image-and-stats--6Nbsa"}).find_all("div", class_="ud-block-list-item-content")[2].text)
        except:
            detail_data["instructor_students"].append("N/A")
        try:
            detail_data["instructor_courses"].append(soup.find("div", {"class": "instructor--instructor__image-and-stats--6Nbsa"}).find_all("div", class_="ud-block-list-item-content")[3].text)
        except:
            detail_data["instructor_courses"].append("N/A")
        try:
            detail_data["instructor_url"].append("https://www.udemy.com" + soup.find("div", class_="ud-heading-lg ud-link-underline instructor--instructor__title--S9oZ4",).find("a").get("href"))
        except:
            detail_data["instructor_url"].append("N/A")
    # Return the parsed data as a dictionary
    return detail_data


def parse_instructor_links(instructor_html_content: list):
    """
    Parses the HTML content of instructor links and extracts various details about the instructors.

    Parameters:
    - instructor_html_content (list): A list of HTML content strings representing instructor pages.

    Returns:
    - instructor_detail_data (dict): A dictionary containing the extracted details of the instructors. The dictionary has the following keys:
        - instructor_name (list): A list of names of the instructors.
        - instructor_website (list): A list of websites of the instructors.
        - twitter (list): A list of Twitter handles of the instructors.
        - linkedin (list): A list of LinkedIn profiles of the instructors.
        - facebook (list): A list of Facebook profiles of the instructors.
        - youtube (list): A list of YouTube channels of the instructors.
        - instructor_url (list): A list of URLs of the instructor pages.

    If any of the details cannot be found in the HTML content, "N/A" is appended to the corresponding list.
    """
    # Initialize an empty dictionary to store the parsed data
    instructor_detail_data = {
        "instructor_name": [],
        "instructor_website": [],
        "twitter": [],
        "linkedin": [],
        "facebook": [],
        "youtube": [],
        "instructor_url": [],
    }
    # Loop through each HTML content in the list
    for html_content in instructor_html_content:
        if html_content is None:
            continue  # Skip None values
        
        # Use BeautifulSoup to parse the HTML content
        soup = BeautifulSoup(html_content, "html.parser")

        # Extract various details about the instructor
        try:
            instructor_detail_data["instructor_name"].append(soup.find("h1", {"class": "ud-heading-serif-xxxl"}).text)
        except:
            instructor_detail_data["instructor_name"].append("N/A")
        try:
            instructor_detail_data["instructor_website"].append(soup.find("div", {"class": "instructor-profile--social-links--02JZE"}).find("a", {"data-purpose": "personal-website-link"}).get("href"))
        except:
            instructor_detail_data["instructor_website"].append("N/A")
        try:
            instructor_detail_data["twitter"].append(soup.find("div", {"class": "instructor-profile--social-links--02JZE"}).find("a", {"data-purpose": "twitter-link"}).get("href"))
        except:
            instructor_detail_data["twitter"].append("N/A")
        try:
            instructor_detail_data["linkedin"].append(soup.find("div", {"class": "instructor-profile--social-links--02JZE"}).find("a", {"data-purpose": "linkedin-link"}).get("href"))
        except:
            instructor_detail_data["linkedin"].append("N/A")
        try:
            instructor_detail_data["facebook"].append(soup.find("div", {"class": "instructor-profile--social-links--02JZE"}).find("a", {"data-purpose": "facebook-link"}).get("href"))
        except:
            instructor_detail_data["facebook"].append("N/A")
        try:
            instructor_detail_data["youtube"].append(soup.find("div", {"class": "instructor-profile--social-links--02JZE"}).find("a", {"data-purpose": "youtube-link"}).get("href"))
        except:
            instructor_detail_data["youtube"].append("N/A")
        try:
            instructor_detail_data["instructor_url"].append(soup.find("meta", property="og:url").get("content"))
        except:
            instructor_detail_data["instructor_url"].append("N/A")
    # Return the parsed data as a dictionary
    return instructor_detail_data


def start_parse_course_links(html_content: list):
    """
    Parses course links and wanted expressions from the HTML content of each page.
    
    Args:
        html_content (list): A list of HTML contents containing course links.
        
    Returns:
        tuple: A tuple containing two lists. The first list contains the parsed course links, 
        and the second list contains the parsed wanted expressions.
    """
    # Start parsing course links
    complet_list_urls: list = []
    complet_list_expressions: list = []
    
    logger.info("\n\t*** Start parsing course links ***")
    start_time_parse_course: datetime = datetime.now()
    
    # Parse course links and wanted expressions from the HTML content of each page  
    for html in html_content:
        
        # If the HTML content is not None then parse it else skip
        if html is not None:
            
            # Parse course links and wanted expressions from the HTML content
            course_links, list_wanted_expressions = parse_course_links(html)
            
            # Append the parsed course links and wanted expressions to the lists
            complet_list_urls.extend(course_links)
            complet_list_expressions.extend(list_wanted_expressions)
    
    end_time_parse_course: datetime = datetime.now()
    logger.info(f"\t*** Done parsing course links in {end_time_parse_course - start_time_parse_course} ***")
    return complet_list_urls, complet_list_expressions


def start_parsing_detail_links(detail_html_contents: list):
    """
    Parses detail links from the given HTML contents and returns the parsed detail data.

    Args:
        detail_html_contents (list): A list of HTML contents containing detail links.

    Returns:
        list: A list of parsed detail data.

    Raises:
        None

    Example:
        >>> start_parsing_detail_links(["<html><body><h3>Detail 1</h3><h3>Detail 2</h3></body></html>"])
        [{'course_title': 'Detail 1', 'course_subtitle': None, 'course_num_students': None, 'course_rating': None}, {'course_title': 'Detail 2', 'course_subtitle': None, 'course_num_students': None, 'course_rating': None}]
    """
    # Start parsing detail links
    logger.info("\n\t*** Start parsing details ***")
    start_time_parse_detail: datetime = datetime.now()
    
    detail_data: list = parse_detail_course_links(detail_html_contents)
    
    end_time_parse_detail: datetime = datetime.now()
    logger.info(f"\t*** Time to parse detail: {end_time_parse_detail - start_time_parse_detail} ***")
    return detail_data


def start_parsing_instructor_links(detail_html_contents_instructor: str):
    """
    Parses instructor links from the given HTML contents and returns the parsed instructor details.

    Args:
        detail_html_contents_instructor (str): The HTML contents containing instructor links.

    Returns:
        list: A list of parsed instructor details.

    Raises:
        None

    Example:
        >>> start_parsing_instructor_links("<html><body><h3>Instructor 1</h3><h3>Instructor 2</h3></body></html>")
        [{'name': 'Instructor 1', 'url': None}, {'name': 'Instructor 2', 'url': None}]
    """
    # Start parsing instructor links
    logger.info("\n\t*** Start parsing instructor details ***")
    start_time_parse_instructor: datetime = datetime.now()
    
    instructor_detail_data: list = parse_instructor_links(detail_html_contents_instructor)
    
    end_time_parse_instructor: datetime = datetime.now()
    logger.info(f"\t*** Time to parse instructor details: {end_time_parse_instructor - start_time_parse_instructor} ***")
    return instructor_detail_data
