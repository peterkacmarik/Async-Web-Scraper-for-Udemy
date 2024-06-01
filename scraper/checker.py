from datetime import datetime
from database.models import DatabaseManagerSettings, CombinedData, InstructorDetails, DetailCourseLinks, CourseLinks
import pandas as pd
import logging
import sys
import os
import logging
from logs import logger
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Load logger settings from .env file
LOG_DIR_CHECK = os.getenv('LOG_DIR_CHECK')

# Create logger object
logger = logger.get_logger(log_file=LOG_DIR_CHECK, log_level=logging.INFO)

# Create logger object with INFO level
# logger = logging.getLogger(__name__)
# logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class CheckRecords:
    def __init__(self) -> None:
        """
        Initializes the CheckNewItems class.
        """
        self.db_manager_settings = DatabaseManagerSettings()

    def compare_data_db(self, complet_list_urls: list, complet_list_expressions: list):
        """
        Compares the given lists of URLs and expressions with the database.

        Args:
            complet_list_urls (list): A list of URLs to compare with the database.
            complet_list_expressions (list): A list of expressions corresponding to the URLs.

        Returns:
            pd.DataFrame or None: A dataframe containing the new details to insert into the database, 
            or None if no new details are found.

        Raises:
            Exception: If an unexpected error occurs during the comparison process.

        Prints:
            - "\t*** No new data to insert ***" if no new details are found.
            - Log messages indicating the new URLs found and the URLs already existing in the database.
            - Elapsed time for comparing details with database.
        """
        logger.info("\n\t*** Start comparing urls with database ***")
        
        # Create dataframe from lists
        df_merged_course_links: pd.DataFrame = pd.DataFrame({"url": complet_list_urls,"wanted_expression": complet_list_expressions,})
        
        # Check if dataframe is empty exit the program
        if df_merged_course_links.empty:
            logger.info("\t*** No new data to insert ***")
            exit()

        # Create blank dataframe to store new details
        column_names: list = [
            "url",
            "wanted_expression",
        ]
        # Dataframe to insert new details
        df_to_insert: pd.DataFrame = pd.DataFrame(columns=column_names)
        
        # Start comparing data with database
        start_time_compare: datetime = datetime.now()

        try:
            # Iterate over each row in the dataframe and check if the URL already exists in the database
            for index, row in df_merged_course_links.iterrows():
                # Get URL
                url: str = row["url"]
                # Check if URL already exists in the database
                df_database: pd.DataFrame = self.db_manager_settings.read_data(model=CourseLinks, conditions=CourseLinks.url == url)

                # If the URL does not exist in the database, add it to the dataframe
                if url not in df_database["url"].values:
                    logger.info(f"New URL found: {url}")
                    
                    # If a new URL is found, return the corresponding row
                    row_to_insert: pd.DataFrame = pd.DataFrame([row.values], columns=column_names)
                    df_to_insert: pd.DataFrame = pd.concat([df_to_insert, row_to_insert], ignore_index=True)

                # If the URL already exists in the database, skip it
                if url in df_database["url"].values:
                    logger.info(f"Skipping... URL already exists in the database: {url}")
                    continue
        
        # If an error occurs, log the error and raise it
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise
        
        # Close the database connection
        self.db_manager_settings.close_connection()
        
        end_time_compare: datetime = datetime.now()
        logger.info(f"Elapsed time for comparing details with database: {end_time_compare - start_time_compare}")
        
        # Return dataframe with new details if any exists else exit the program
        if df_to_insert.empty:
            logger.info("\t*** No new data to insert ***")
            exit()
        else:
            return df_to_insert
