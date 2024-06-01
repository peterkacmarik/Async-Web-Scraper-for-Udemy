from datetime import datetime
import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Table, Text, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from sqlalchemy.orm import relationship

# from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError


# Database tables definition (declarative base)
Base = declarative_base()

# Load environment variables
load_dotenv()


class CourseLinks(Base):
    # Set the table name and primary key column name (automatically generated if not specified)
    __tablename__ = os.getenv("DATABASE_TABLE_SQLITE_COURSE_LINKS", "course_links")   # Set the table name
    id = Column(Integer, primary_key=True, autoincrement=True)   # Set the primary key
    url = Column(String)  # Set the column name
    wanted_expression = Column(String)  # Set the column name
    created_at = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        # Return a string representation of the object
        return f"Course: {self.url}"


class DetailCourseLinks(Base):
    # Set the table name and primary key column name (automatically generated if not specified)
    __tablename__ = os.getenv("DATABASE_TABLE_SQLITE_DETAIL_COURSE_LINKS", "detail_course_links")   # Set the table name
    id = Column(Integer, primary_key=True, autoincrement=True)   # Set the primary key
    course_title = Column(String)  # Set the column name
    course_subtitle = Column(String)  # Set the column name
    # course_price = Column(String)  # Set the column name
    course_num_students = Column(String)  # Set the column name
    course_rating = Column(String)  # Set the column name
    course_created_by = Column(String)  # Set the column name
    course_last_updated = Column(String)  # Set the column name
    course_url = Column(String)  # Set the column name
    instructor_name = Column(String)  # Set the column name
    instructor_ratings = Column(String)  # Set the column name
    instructor_reviews = Column(String)  # Set the column name
    instructor_students = Column(String)  # Set the column name
    instructor_courses = Column(String)  # Set the column name
    instructor_url = Column(String)  # Set the column name
    created_at = Column(DateTime, default=datetime.now)

    def __repr__(self):
        # Return a string representation of the object
        return f"Course: {self.course_title}, Instructor: {self.instructor_name}"


class InstructorDetails(Base):
    # Set the table name and primary key column name (automatically generated if not specified)
    __tablename__ = os.getenv("DATABASE_TABLE_SQLITE_INSTRUCTOR_DETAIL", "instructor_details")   # Set the table name
    id = Column(Integer, primary_key=True, autoincrement=True)   # Set the primary key
    instructor_name = Column(String)  # Set the column name
    instructor_website = Column(String)  # Set the column name
    twitter = Column(String)  # Set the column name
    linkedin = Column(String)  # Set the column name
    facebook = Column(String)  # Set the column name
    youtube = Column(String)  # Set the column name
    instructor_url = Column(String)  # Set the column name
    created_at = Column(DateTime, default=datetime.now)

    def __repr__(self):
        # Return a string representation of the object
        return f"Instructor: {self.instructor_name}"


class MergeData(Base):
    # Set the table name and primary key column name (automatically generated if not specified)
    __tablename__ = os.getenv("DATABASE_TABLE_SQLITE_MERGE_DATA", "merge_data")   # Set the table name
    id = Column(Integer, primary_key=True, autoincrement=True)   # Set the primary key
    
    url = Column(String)  # Set the column name
    wanted_expression = Column(String)  # Set the column name
    
    course_title = Column(String)  # Set the column name
    course_subtitle = Column(String)  # Set the column name
    # course_price = Column(String)  # Set the column name
    course_num_students = Column(String)  # Set the column name
    course_rating = Column(String)  # Set the column name
    course_created_by = Column(String)  # Set the column name
    course_last_updated = Column(String)  # Set the column name
    course_url = Column(String)  # Set the column name
    instructor_name = Column(String)  # Set the column name
    instructor_ratings = Column(String)  # Set the column name
    instructor_reviews = Column(String)  # Set the column name
    instructor_students = Column(String)  # Set the column name
    instructor_courses = Column(String)  # Set the column name
    instructor_url = Column(String)  # Set the column name
    
    instructor_name = Column(String)  # Set the column name
    instructor_website = Column(String)  # Set the column name
    twitter = Column(String)  # Set the column name
    linkedin = Column(String)  # Set the column name
    facebook = Column(String)  # Set the column name
    youtube = Column(String)  # Set the column name
    instructor_url = Column(String)  # Set the column name
    created_at = Column(DateTime, default=datetime.now)
    

class CombinedData(Base):
    # Set the table name and primary key column name (automatically generated if not specified)
    __tablename__ = os.getenv("DATABASE_TABLE_SQLITE_COMBINED_DATA", "combined_data")   # Set the table name
    id = Column(Integer, primary_key=True, autoincrement=True)   # Set the primary key
    
    # ForeignKey columns
    course_link_id = Column(Integer, ForeignKey('course_links.id'))
    detail_course_link_id = Column(Integer, ForeignKey('detail_course_links.id'))
    instructor_detail_id = Column(Integer, ForeignKey('instructor_details.id'))
    
    # Relationships
    course_link = relationship("CourseLinks", backref="combined_data")
    detail_course_link = relationship("DetailCourseLinks", backref="combined_data")
    instructor_detail = relationship("InstructorDetails", backref="combined_data")


class DatabaseManagerSettings:
    def __init__(self) -> None:
        """
        Initializes the DatabaseManagerSettings class.

        This method creates an engine using the `create_engine` function from the `sqlalchemy` module, 
        passing the database URL from the `DATABASE_URL_SQLITE` environment variable as an argument. 
        It then creates a session using the `sessionmaker` function from the `sqlalchemy.orm` module, 
        binding it to the engine. Finally, it creates a session using the `Session` class and assigns it 
        to the `session` attribute of the class.

        Parameters:
            None

        Returns:
            None
        """
        self.engine = create_engine(os.getenv("DATABASE_URL_SQLITE"))   # Create an engine
        self.Session = sessionmaker(bind=self.engine)   # Create a session
        self.session = self.Session()   # Create a session
        Base.metadata.create_all(self.engine)

    def create_table(self, table: Table):
        """
        Create a table in the database using the provided Table object.

        Args:
            table (Table): The Table object representing the table to be created.

        Returns:
            None
        """
        # Create a table in the database using the provided Table object
        try:
            table.create(self.engine, checkfirst=True)
            print(f"Table created successfully")
        except OperationalError as e:
            print(f"Error creating table: {e}")

    def insert_combined_data(self, df_course_links: pd.DataFrame, df_detail_course_links: pd.DataFrame, df_instructor_detail: pd.DataFrame):  
        for index in range(len(df_course_links)):
            # Najskôr nájdite alebo vytvorte záznamy v pôvodných modeloch
            course_link = CourseLinks(
                url=df_course_links.iloc[index]['url'],
                wanted_expression=df_course_links.iloc[index]['wanted_expression']
            )
            self.session.add(course_link)
            self.session.flush()
            
            detail_course_link = DetailCourseLinks(
                course_title=df_detail_course_links.iloc[index]['course_title'],
                course_subtitle=df_detail_course_links.iloc[index]['course_subtitle'],
                course_num_students=df_detail_course_links.iloc[index]['course_num_students'],
                course_rating=df_detail_course_links.iloc[index]['course_rating'],
                course_created_by=df_detail_course_links.iloc[index]['course_created_by'],
                course_last_updated=df_detail_course_links.iloc[index]['course_last_updated'],
                course_url=df_detail_course_links.iloc[index]['course_url'],
                instructor_name=df_detail_course_links.iloc[index]['instructor_name'],
                instructor_ratings=df_detail_course_links.iloc[index]['instructor_ratings'],
                instructor_reviews=df_detail_course_links.iloc[index]['instructor_reviews'],
                instructor_students=df_detail_course_links.iloc[index]['instructor_students'],
                instructor_courses=df_detail_course_links.iloc[index]['instructor_courses'],
                instructor_url=df_detail_course_links.iloc[index]['instructor_url']
            )
            self.session.add(detail_course_link)
            self.session.flush()
            
            instructor_detail = InstructorDetails(
                instructor_name=df_instructor_detail.iloc[index]['instructor_name'],
                instructor_website=df_instructor_detail.iloc[index]['instructor_website'],
                twitter=df_instructor_detail.iloc[index]['twitter'],
                linkedin=df_instructor_detail.iloc[index]['linkedin'],
                facebook=df_instructor_detail.iloc[index]['facebook'],
                youtube=df_instructor_detail.iloc[index]['youtube'],
                instructor_url=df_instructor_detail.iloc[index]['instructor_url']
            )
            # Pridajte ich do session
            self.session.add(instructor_detail)
            self.session.flush()
            
            # Teraz vytvorte záznam v CombinedData
            combined_data = CombinedData(
                course_link_id=course_link.id,
                detail_course_link_id=detail_course_link.id,
                instructor_detail_id=instructor_detail.id
            )
            self.session.add(combined_data)

        # Nakoniec commitnite všetky zmeny
        self.session.commit()

    def insert_data(self, df: pd.DataFrame, Model: declarative_base):
        """
        Insert data into the database using the provided DataFrame and Model.

        Args:
            df (pd.DataFrame): The DataFrame containing the data to be inserted.
            Model (declarative_base): The SQLAlchemy model representing the table schema.

        Returns:
            None
        """
        # Insert data into the database using the provided DataFrame and Model
        for _, row in df.iterrows():
            obj = Model(**row.to_dict())    # Convert the row to a dictionary and pass it to the Model
            self.session.add(obj)   # Add the object to the session
        self.session.commit()   # Commit the changes to the database

    def read_data(self, model, conditions=None):
        """
        Read data from the database using the provided Model and conditions.

        Parameters:
            model (DeclarativeMeta): The model class to query.
            conditions (Optional[BinaryExpression]): The optional conditions to filter the query results.

        Returns:
            pandas.DataFrame: The queried data as a DataFrame.
        """
        # Read data from the database using the provided Model and conditions
        query = self.session.query(model)

        # Apply the conditions if provided
        if conditions:
            query = query.filter(conditions)

        # Execute the query and return the results as a DataFrame
        data = pd.read_sql(query.statement, self.session.bind)
        return data

    def update_data(self, model, updates):
        """
        Updates data in the database using the provided Model and updates.

        Parameters:
            model (DeclarativeMeta): The SQLAlchemy model representing the table schema.
            updates (dict): A dictionary containing the column names and their corresponding new values.

        Returns:
            None
        """
        # Update data in the database using the provided Model and updates
        self.session.query(model).update(updates, synchronize_session=False)
        self.session.commit()   # Commit the changes to the database

    def delete_all_data(self, model):
        """
        Deletes all data from the database using the provided Model.

        Parameters:
            model (DeclarativeMeta): The SQLAlchemy model representing the table schema.

        Returns:
            None
        """
        # Delete all data from the database using the provided Model
        self.session.query(model).delete()
        self.session.commit()   # Commit the changes to the database

    def close_connection(self):
        """
        Close the database connection.

        This method closes the session object, which releases any resources held by the session and closes the database connection.

        Parameters:
            self (DatabaseManagerSettings): The instance of the DatabaseManagerSettings class.

        Returns:
            None
        """
        # Close the database connection
        self.session.close()


# db_manager_settings = DatabaseManagerSettings()

# Vymazanie tabuliek
# db_manager_settings.delete_all_data(CourseLinks)
# db_manager_settings.delete_all_data(DetailCourseLinks)
# db_manager_settings.delete_all_data(InstructorDetails)
# db_manager_settings.delete_all_data(CombinedData)
# db_manager_settings.delete_all_data(MergeData)

# Vypis DataFrame
# print(db_manager_settings.read_data(CourseLinks))

# Vytvorenie tabuliek
# db_manager_settings.create_table(CourseLinks.__table__)
# db_manager_settings.create_table(DetailCourseLinks.__table__)
# db_manager_settings.create_table(InstructorDetails.__table__)
# db_manager_settings.create_table(CombinedData.__table__)
# db_manager_settings.create_table(MergeData.__table__)

# Vloženie dát
# db_manager_settings.insert_data(df_course_links, CourseLinks)
# db_manager_settings.insert_data(df_detail_course_links, DetailCourseLinks)
# db_manager_settings.insert_data(df_instructor_detail, InstructorDetails)
# db_manager_settings.insert_combined_data(df_course_links, df_detail_course_links, df_instructor_detail)


# Čítanie dát
# search_data: pd.DataFrame = db_manager_settings.read_data(CourseLinks, conditions=[CourseLinks.wanted_expression == 'make money online'])
# print(search_data['wanted_expression'][0])
# print(search_data['url'][0])

# search_data: pd.DataFrame = db_manager_settings.read_data(CourseLinks)
# print(search_data['wanted_expression'][0])


# Aktualizácia dát
# x = 11
# c = 14
# db_manager_settings.update_data(CourseLinks, {'wanted_expression': x})
# db_manager_settings.update_data(CourseLinks, {'url': c})


# Vymazanie dát
# db_manager_settings.delete_all_data(CourseLinks)


# Zatvorenie spojenia
# db_manager_settings.close_connection()
