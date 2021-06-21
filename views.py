from models import HEADR, CONSU, Base
from database import engine
from sqlalchemy.orm import sessionmaker
import datetime

Session = sessionmaker(bind=engine)
s= Session()



#How many meters are in the dataset



s.close()