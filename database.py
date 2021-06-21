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




#Connecting to database
engine = create_engine(Database_URI, echo=False)

#Binding session to engine i.e specific database
Session= sessionmaker(bind=engine)
s = Session() # Allows python to interact with PostgreSQL



#Creating table after importing original Base from models module
Base.metadata.create_all(bind=engine)
s.commit()    #uploading newly created table



#File paths showing where the different files are
Original_data_folder = "C:/Users/shafi/Dropbox/Data Science Projects/GAZPROM/GazProm-Data-Engineering-Project/sample_data/"
uploaded_data_folder= "C:/Users/shafi/Dropbox/Data Science Projects/GAZPROM/GazProm-Data-Engineering-Project/Uploaded data/"
Bad_data_folder= "C:/Users/shafi/Dropbox/Data Science Projects/GAZPROM/GazProm-Data-Engineering-Project/Bad data/"
files = os.listdir(Original_data_folder)

#Function that converts the time to correct format
def time_convertor(raw_time):
    length = len(str(int(raw_time)))  #length of time string
    if length < 4:  #if the length is lessthan 4 i.e not not in a HH:MM format
        missing_values = 4 - length    #amount of values missing
        raw_time = '0'*missing_values + str(int(raw_time))   #missing values added at the front to provide the data in an accurate HH:MM format
        Time = datetime.strptime( raw_time, "%H%M").time()   #Time converison
        return Time
    else:
        raw_time = str(int(raw_time))  #Changing time to an integer to get rid of the digits after the point
        Time = datetime.strptime( raw_time, "%H%M").time() #Time converison
        return Time

for file in files:
    df = pd.read_csv(Original_data_folder+file) #Pandas dataframe for a given file
    To_Head = list(df.columns) #List of columns in dataframe
    N_rows = len(df.index) - 1 #Number of rows in dataframe excluding the footer


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
                    #Row in database with matching parameters is overwritten
                    over_write_result = s.query(CONSU).filter(CONSU.Meter_Number == float(row[1]), CONSU.Measurement_Date == row[2],
                                                              CONSU.Measurement_Time ==row[3]).update({CONSU.Record_Identifier:row[0],
                                                               CONSU.Meter_Number: float(row[1]), CONSU.Measurement_Date: row[2],
                                                                CONSU.Measurement_Time:row[3],CONSU.Consumption:float(row[4]),
                                                                CONSU.Header_id:headers.id}, synchronize_session="evaluate")

                else:
                    consus = CONSU(Record_Identifier=row[0], Meter_Number=float(row[1]), Measurement_Date=row[2],
                                   Measurement_Time=row[3], Consumption=float(row[4]), Header_id=headers.id)
                    s.add(consus) #Inserting rows in  table

            # #Moving file to new location to prevent duolication
            Old_location = Original_data_folder + file #Original data folder
            New_location = uploaded_data_folder + file #Target data folder
            shutil.move(Old_location, New_location)

        except IntegrityError:  #Python returns Integrity error if the File Genreation Number is non-unique
            s.rollback() #Rolls back the current session
            # Moving repeated file to uploaded data folder
            Old_location = Original_data_folder + file  # Original data folder
            Target_location = Bad_data_folder+file
            shutil.move(Old_location,Target_location)

    else:
        # Moving bad data to bad data folder
        Old_location = Original_data_folder + file  # Original data folder
        Target_location = Bad_data_folder + file  #Target location for bad data
        shutil.move(Old_location, Target_location)

#Pushing all the changes to the Database
s.commit()

#Ending any transactions in progress
s.close()