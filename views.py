from models import HEADR, CONSU, Base
from database import engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func,distinct, desc

Session = sessionmaker(bind=engine)
s= Session()



#How many meters are in the dataset?
meters = s.query(func.count(distinct(CONSU.Meter_Number))).all()

#What is all the data for a given meter i.e How many records are there for meter 5 in the dataset?
data_for_given_meter = s.query(func.count(CONSU.Meter_Number)).filter(CONSU.Meter_Number==5).all()

#How many files have we received?
files_received = s.query(func.count(HEADR.id)).all()

#What was the last file to be received
last_file_uploaded = s.query(HEADR).order_by( desc(HEADR.Upload_Datetime)).first()
last_file_uploaded.__dict__['File_Generation_Number']


#closing session
s.close()