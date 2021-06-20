from sqlalchemy import Column, Integer, String, Date,Float,Time
from sqlalchemy import ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship




Base = declarative_base()

class HEADR(Base):
    __tablename__ = "Header"
    id = Column(Integer, primary_key=True, autoincrement=True)
    Record_Identifier = Column(String, nullable=True)
    File_Type = Column(String)
    Company_ID = Column(String(50))
    File_Creation_Date = Column(Date)
    File_Creation_Time = Column(Time)
    File_Generation_Number= Column(String, unique=True, nullable=False)
    



class CONSU(Base):
    __tablename__ ="CONSU"
    id = Column(Integer, primary_key=True,autoincrement=True)
    Record_Identifier = Column(String)
    Meter_Number = Column(Integer)
    Measurement_Date = Column(Date)
    Measurement_Time = Column(Time)
    Consumption= Column(Float)
    Header_id = Column(Integer,ForeignKey("Header.id")) #Foreign key added

    HEADR = relationship("HEADR")
