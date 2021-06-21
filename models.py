from sqlalchemy import Column, Integer, String, Date,Float,Time,DateTime
from sqlalchemy import ForeignKey, Identity
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import datetime



#Declaring base to define classes
Base = declarative_base()


#Table class that will construct the first table with the main records of each file
class HEADR(Base):
    __tablename__ = "Header"
    id = Column(Integer, Identity(always=False,start=1,increment=1),primary_key=True)
    Upload_Datetime = Column(DateTime, default=datetime.datetime.now) #Time when file is received in Database
    Number_of_rows = Column(Integer) #Number of rows per file excluding footer
    Record_Identifier = Column(String) #Setting data types to validate the data
    File_Type = Column(String)
    Company_ID = Column(String)
    File_Creation_Date = Column(Date)
    File_Creation_Time = Column(Time)
    File_Generation_Number= Column(String, unique=True, nullable=False)
    


#Table class that wil construct the second table to populate the rows from each file
class CONSU(Base):
    __tablename__ ="CONSU"
    id = Column(Integer,  Identity(always=False, start=1, increment=1) ,primary_key=True)
    Record_Identifier = Column(String)
    Meter_Number = Column(Integer) #Setting data types to validate the data
    Measurement_Date = Column(Date)
    Measurement_Time = Column(Time)
    Consumption= Column(Float)
    Header_id = Column(Integer,ForeignKey("Header.id")) #Foreign key added from Header table

    HEADR = relationship("HEADR")

