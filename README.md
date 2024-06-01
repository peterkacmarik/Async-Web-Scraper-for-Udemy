
# Async Web Scraper for Udemy

This project is an asynchronous web scraper designed to collect course data from Udemy. It fetches course links, detailed course information, and instructor details, and stores the collected data in a database. The data is also saved in CSV and Excel formats for further analysis.

## Features

- Asynchronously fetches course links from Udemy search results.
- Parses detailed course information and instructor details.
- Compares new data with existing database records to avoid duplicates.
- Stores data in a database and exports it to CSV and Excel files.
- Uses Playwright for browser automation and BeautifulSoup for HTML parsing.

## Requirements

- Python 3.8+
- [Playwright](https://playwright.dev/python/docs/intro)
- [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- [pandas](https://pandas.pydata.org/)
- [python-dotenv](https://pypi.org/project/python-dotenv/)
- [SQLAlchemy](https://www.sqlalchemy.org/)
- [asyncio](https://docs.python.org/3/library/asyncio.html)
- [logging](https://docs.python.org/3/library/logging.html)

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/peterkacmarik/async-web-scraper-udemy.git
    cd async-web-scraper-udemy
    ```

2. Install the required packages:

    ```bash
    pip install -r requirements.txt
    ```

3. Install Playwright:

    ```bash
    playwright install
    ```

4. Set up your environment variables:

    Create a `.env` file in the root directory and add your environment variables as shown below:

    ```env
    DATABASE_URL=your_database_url
    ```

## Usage

1. Customize the script parameters:

    - `start_page`: The starting page number for the Udemy search results.
    - `end_page`: The ending page number for the Udemy search results.
    - `expression`: The search term to look for courses on Udemy.
    - `max_concurrent_tasks`: The maximum number of concurrent tasks for fetching data.
    - `timeout`: The timeout duration for each request in milliseconds.

2. Run the script:

    ```bash
    python main.py
    ```

    Alternatively, you can run the script directly:

    ```bash
    asyncio.run(main_script(urls, timeout, max_concurrent_tasks))
    ```

## Example

To search for courses related to "ChatGPT" and fetch data from the first page of the search results:

```python
if __name__ == "__main__":
    start_page = 1
    end_page = 1
    expression = "ChatGPT"
    max_concurrent_tasks = 4
    timeout = 30000

    urls = [
        f"https://www.udemy.com/courses/search/?kw={expression}&p={page}&q={expression}&src=sac"
        for page in range(start_page, end_page + 1)
    ]

    asyncio.run(main_script(urls, timeout, max_concurrent_tasks))
```

## Logging

The script uses Python's built-in `logging` module to log information, warnings, and errors. You can customize the logging settings in the `logger` configuration.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

- [Playwright](https://playwright.dev/python/docs/intro)
- [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- [pandas](https://pandas.pydata.org/)
- [python-dotenv](https://pypi.org/project/python-dotenv/)
- [SQLAlchemy](https://www.sqlalchemy.org/)

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your changes.

## Contact

For any questions or feedback, please contact [peterkacmarik@gmail.com](mailto:peterkacmarik@gmail.com).
