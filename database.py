##### database.py ####

from config import Database_URI
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
import pandas as pd
from datetime import datetime
from models import HEADR,CONSU,Base
from sqlalchemy.exc import IntegrityError
import shutil




#Initialising engine
engine = create_engine(Database_URI, echo=False)

#Binding session to engine
Session= sessionmaker(bind=engine)
s = Session()



#Creating table after import original base from models.py
Base.metadata.create_all(bind=engine)
s.commit()    #Committing changes




sample_data_path = "C:/Users/shafi/Dropbox/Data Science Projects/GAZPROM/GazProm-Data-Engineering-Project/sample_data/"
uploaded_data_path= "C:/Users/shafi/Dropbox/Data Science Projects/GAZPROM/GazProm-Data-Engineering-Project/Uploaded data/"
Bad_data_path= "C:/Users/shafi/Dropbox/Data Science Projects/GAZPROM/GazProm-Data-Engineering-Project/Bad data/"
files = os.listdir(sample_data_path)

#Function that converts the time to correct format
def time_convertor(raw_time):
    length = len(str(int(raw_time)))
    if length < 4:
        missing_values = 4 - length    #amount of values missing
        raw_time = '0'*missing_values + str(int(raw_time))   #missing values added at the front to provide the data in an accurate HH:MM format
        Time = datetime.strptime( raw_time, "%H%M").time()
        return Time
    else:
        raw_time = str(int(raw_time))
        Time = datetime.strptime( raw_time, "%H%M").time()
        return Time

for file in files:
    df = pd.read_csv(sample_data_path+file)
    To_Head = list(df.columns) #List of columns in dataframe
    N_rows = len(df.index)-1 #Number of rows in dataframe excluding the footer
    #if function that checks if whole file was tranferred i.e headers and footers
    if (To_Head[0] == 'HEADR') & (df.iloc[-1][0] == 'TRAIL'):
        df.drop(df.columns[-1], axis=1, inplace=True) #Dropping File_Generation_Number column since its the last column
        df.dropna(inplace=True, how='any', axis=0)   #Dropping footer row
        Creation_date = datetime.strptime(To_Head[3], "%Y%m%d").date() #Date conversion
        Creation_time = datetime.strptime(To_Head[4], "%H%M%S").time() #Time conversion
        To_Head.pop(3) #Removing date string from list
        To_Head.pop(3) #Removing time string from list
        To_Head.insert(3, Creation_date)  #inserting the newly converted date back into the list
        To_Head.insert(4, Creation_time)  #inserting the newly converted Time back into the list
        df[df.columns[3]] = df[df.columns[3]].apply(lambda x: time_convertor(x))
        df[df.columns[2]] = df[df.columns[2]].apply(lambda x: datetime.strptime(str(int(x)), "%Y%m%d").date())

        #List of rows of records as tuples
        rows = list(map(tuple, df.to_numpy()))

        # Try block is employed to deal with non-unique File Generation Numbers(FGN). To make sure the same file isn't uploaded again
        try:
            headers = HEADR(
                Record_Identifier=To_Head[0], Number_of_rows = N_rows,File_Type=To_Head[1], Company_ID=To_Head[2],
                File_Creation_Date=To_Head[3], File_Creation_Time=To_Head[4], File_Generation_Number=To_Head[5])
            s.add(headers)
            s.commit()

            for row in rows:
                #Overwrite query that checks to see if the data being uploaded shares the same meter number and measurement datetime
                over_write_query =s.query(CONSU).filter(CONSU.Meter_Number == float(row[1]),
                                                        CONSU.Measurement_Date == row[2], CONSU.Measurement_Time ==row[3] ).first()

                #if function that checks if query returned isn't empty i.e the data is going to be overwritten
                if over_write_query is not None:
                    over_write_result = s.query(CONSU).filter(CONSU.Meter_Number == float(row[1]), CONSU.Measurement_Date == row[2], CONSU.Measurement_Time ==row[3])\
                        .update({CONSU.Record_Identifier:row[0], CONSU.Meter_Number: float(row[1]), CONSU.Measurement_Date: row[2], CONSU.Measurement_Time:row[3],
                                 CONSU.Consumption:float(row[4]), CONSU.Header_id:headers.id}, synchronize_session="evaluate")
                else:
                    consus = CONSU(Record_Identifier=row[0], Meter_Number=float(row[1]), Measurement_Date=row[2],
                                   Measurement_Time=row[3], Consumption=float(row[4]), Header_id=headers.id)
                    s.add(consus)

            # #Moving file to new location to prevent duolication
            Old_location = sample_data_path + file #Original data folder
            New_location = uploaded_data_path + file #Target data folder
            shutil.move(Old_location, New_location)

        except IntegrityError:  #Python returns Integrity error if the File Genreation Number is non-unique
            s.rollback()
            # Moving repeated file to uploaded data folder
            Old_location = sample_data_path + file  # Original data folder
            Target_location = Bad_data_path+file
            shutil.move(Old_location,Target_location)

    else:
        # Moving bad data to bad data folder
        Old_location = sample_data_path + file  # Original data folder
        Target_location = Bad_data_path + file
        shutil.move(Old_location, Target_location)

s.commit()

s.close()