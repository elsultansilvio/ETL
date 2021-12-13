import os, shutil, time, sys
import pandas as pd
from pandas.core.frame import DataFrame

start_timer = time.time()
STARTUP_WAIT = 10  # programma gaat bij opstart x seconden wachten alvorens te starten (voor bv. VPN)
counter = 0 # programma gaat na counter_max re-initialiseren (kijken of inputfile veranderd is)
counter_max = 13
sleep_time = 5  # wachttijd in seconden tussen status check `LastModified` & eventueele `CopyTo()`


def __init__(initFailCounter=0) -> tuple[str,DataFrame]:
    ''' Initialiseer, lees input (excel) in met `SOURCE` en `DESTINATION` van files\n
        Test of file `SOURCE` en `DESTINATION` toegankelijk zijn (voor elke file), neem enkel files mee zonder error\n
        Zet flags `RECENT_FILE_UPDATE` is True & `LAST_TRANSFER` is 0 zodat files meegenomen worden in `CopyTo` flow\n
        Return
        ------
        Path naar bronfile (excel) `SOURCE_inputfile` str & DataFrame `STARTUP_DF_NoError`'''
    # PATH en FILE voor start up excel met op te nemen bestanden
    STARTUP_source_PATH = '.'
    STARTUP_source_FILE = 'INPUT-MtTime.xlsx'

    # reeks concatenates voor PATH en FILE; overbodige columns verwijderen
    SOURCE_inputfile = PathFile(STARTUP_source_PATH,STARTUP_source_FILE)
    STARTUP_DF = pd.read_excel(SOURCE_inputfile, engine='openpyxl')  
    STARTUP_DF = STARTUP_DF[STARTUP_DF['SOURCE_FILE'].notna()]  # neem enkel records waar `SOURCE_FILE` ingevuld is
    if STARTUP_DF['SOURCE_FILE'].isna().all():
        time.sleep(STARTUP_WAIT)
        initFailCounter += 1
        if initFailCounter == 10:
            print('exit, input file niet gevonden')
            exit()
        __init__(initFailCounter)  # probeer opnieuw
    else:
        STARTUP_DF['SOURCE'] = STARTUP_DF.apply(lambda x: PathFile(x['SOURCE_PATH'],x['SOURCE_FILE']), axis=1) 
        STARTUP_DF['DESTINATION'] = STARTUP_DF.apply(lambda x: PathFile(x['DESTINATION_PATH'],x['DESTINATION_FILE']), axis=1)   
        STARTUP_DF.drop(columns=['SOURCE_PATH','SOURCE_FILE','DESTINATION_FILE'], inplace=True) 
        
        # bool die aangeeft of file recent is aangepast
        # Staat bij opstart op True om copy te forceren
        STARTUP_DF['RECENT_FILE_UPDATE'] = True     
        # unixtimestamp van laatste transfer van file
        # Staat bij opstart op 0 om copy te forceren
        STARTUP_DF['LAST_TRANSFER'] = 0

        STARTUP_DF = Get_Status_Wrapper(STARTUP_DF)
        # neem enkel records zonder error in source en/of destination
        STARTUP_DF_NoError = STARTUP_DF[(STARTUP_DF['ERROR_SOURCE'] == 0) & (STARTUP_DF['ERROR_DESTINATION'] == 0)].copy()

        STARTUP_DF_NoError = CopyTo(STARTUP_DF_NoError)  
        return SOURCE_inputfile, STARTUP_DF_NoError

def re__init__(SOURCE, OLD_STARTUP_DF_NoError) -> DataFrame:
    ''' re-initialiseer, lees input (excel) in met `SOURCE` en `DESTINATION` van files\n
        Test of files `SOURCE` en `DESTINATION` toegankelijk zijn (voor elke file), neem enkel files mee zonder error\n
        Zet flag `RECENT_FILE_UPDATE` = True & `LAST_TRANSFER` = 0 zodat NIEUWE files meegenomen worden in `CopyTo` flow\n
        Return
        ------
        Path naar bronfile (excel) `SOURCE_inputfile` str & DataFrame `STARTUP_DF_NoError`\n
        Exception
        ---------
        re-initialiseren faalt -> initialiseer `__init__()`'''
    try:
        # reeks concatenates voor PATH en FILE; overbodige columns verwijderen
        STARTUP_DF = pd.read_excel(SOURCE, engine='openpyxl')
        STARTUP_DF['SOURCE'] = STARTUP_DF.apply(lambda x: PathFile(x['SOURCE_PATH'],x['SOURCE_FILE']), axis=1)
        STARTUP_DF['DESTINATION'] = STARTUP_DF.apply(lambda x: PathFile(x['DESTINATION_PATH'],x['DESTINATION_FILE']), axis=1)   
        STARTUP_DF.drop(columns=['SOURCE_PATH','SOURCE_FILE','DESTINATION_FILE'], inplace=True)  

        STARTUP_DF = Get_Status_Wrapper(STARTUP_DF)   

        # bool die aangeeft of file recent is aangepast
        # Staat bij re__init__ op True om copy te forceren
        STARTUP_DF['RECENT_FILE_UPDATE'] = True
        # unixtimestamp van laatste transfer van file
        # Staat bij opstart op 0 om copy te forceren
        STARTUP_DF['LAST_TRANSFER'] = 0       

        # neem enkel records zonder error in source en/of destination
        STARTUP_DF_NoError = STARTUP_DF[(STARTUP_DF['ERROR_SOURCE'] == 0) & (STARTUP_DF['ERROR_DESTINATION'] == 0)].copy()  
        # vervang op niveau van index (=rij) van excel de opnieuw ingelezen excel
        # indien file van row 1 naar 2 gaat, en row 1 blanco is
        # gaan we hier een error krijgen
        columns = ['RECENT_FILE_UPDATE','LAST_TRANSFER']
        STARTUP_DF_NoError[columns].loc[OLD_STARTUP_DF_NoError.index] = OLD_STARTUP_DF_NoError[columns]
        
        return STARTUP_DF_NoError
    except:
        print("re__init__() failed")
        __init__()  # complete herstart

def PathFile(path, file) -> str:
    ''' concatenate `PATH` en `FILE`. Inputformat: bv. `SOURCE_PATH` = C:/Users/xxx/MIJNFOLDER\n
        `!` geen `/` na `PATH` bij input!\n
        Return
        ------
        `path/file` str'''
    path_file = '{0}/{1}'.format(str(path),str(file))
    return path_file

def LastModified(path_file) -> float:
    ''' wrapper functie os.path.getmtime(), \n 

        Return
        -------
        FILE_last_MODIFIED unixtimestamp'''
    FILE_last_MODIFIED = os.path.getmtime(path_file)    
    return FILE_last_MODIFIED

def Get_Status_Wrapper(df) -> DataFrame:
    ''' wrapper functie die nagaat of `SOURCE` (SOURCE_PATH + SOURCE_FILE)\n
        & `DESTINATION` (DESTINATION_PATH) toegankelijk zijn; en van wanneer laatste `SOURCE` modificatie dateert
        
        Return
        -------
        DataFrame met `ERROR_DESTINATION` bool, `ERROR_SOURCE` bool en `LAST_MODIFIED` unixtimestamp'''
    df = Get_Status_Source(df)
    df = Get_Status_Destination(df)
    return df

def Get_Status_Destination(df) -> DataFrame:
    ''' gaat de status van de `DESTINATION` (DESTINATION_PATH) na\n
            -> `os.listdir()` gaat een error geven indien deze niet toegankelijk is\n

        Return
        -------
        DataFrame met `ERROR_DESTINATION` bool
        bij error `DESTINATION`: ERROR_DESTINATION = True'''

    ERROR_DESTINATION = []
    try:
        df.apply(lambda x: os.listdir(x['DESTINATION_PATH']), axis = 1)
        df['ERROR_DESTINATION'] = False

        return df
    except:
        print('ERROR DESTINATION: niet voor elke file gevonden')
        for dest in df['DESTINATION_PATH']:
            try:
                os.listdir(dest)
                ERROR_DESTINATION.append(False)
            except:
                print('ERROR DESTINATION: {0}'.format(dest))
                ERROR_DESTINATION.append(True)

        df['ERROR_DESTINATION'] = ERROR_DESTINATION
        return df    

def Get_Status_Source(df) -> DataFrame:
    ''' gaat de status van de `SOURCE` (SOURCE_PATH + SOURCE_FILE) na\n
            -> `LastModified()` gaat een error geven indien deze niet toegankelijk is\n
        
        Return
        ------
        DataFrame met `ERROR_SOURCE` bool en `LAST_MODIFIED` unixtimestamp\n
            bij error `SOURCE`: ERROR_SOURCE = True en LAST_MODIFIED= 0\n
        Exception
        ------
        '''
    try:
        df['LAST_MODIFIED'] = df.apply(lambda x: LastModified(x['SOURCE']), axis = 1)
        df['ERROR_SOURCE'] = False
    except:
        print('ERROR SOURCE: niet elke file gevonden') # TD: error handler    
        LAST_MODIFIED, ERROR_SOURCE = RowByRow_Updater(df)
        df['LAST_MODIFIED'] = LAST_MODIFIED
        df['ERROR_SOURCE'] = ERROR_SOURCE

    return df

def RowByRow_Updater(df) -> list:
    ''' catch indien vectoriseren bij `Get_Status_Source()` faalt\n
        gaat record per record af om te kijken welke `SOURCE` faalt
        
        Return
        ------
        List `LAST_MODIFIED` unixtimestamp, List `ERROR_SOURCE` bool\n
        Exception
        ------
        `RowByRow_updater()` faalt (bv. geen locaties in bronfile) -> initialiseer `__init__()`
        '''
    try:
        LAST_MODIFIED = []
        ERROR_SOURCE = []
        for source in df['SOURCE']: 
            try:           
                LAST_MODIFIED.append(LastModified(source))
                ERROR_SOURCE.append(False)
            except:
                LAST_MODIFIED.append(0)
                ERROR_SOURCE.append(True)
        
        return LAST_MODIFIED, ERROR_SOURCE
    except TypeError:   # geen enkele `SOURCE` en `DESTINATION` pair gevonden
        print("RowByRow_updated failed")
        __init__()  # complete herstart

def RecentUpdated(df) -> bool:
    ''' kijkt of `LAST_MODIFIED` groter is dan `LAST_TRANSFER`\n 
            
        Return
        ------ 
        Series bool\n
            LAST_TRANSFER is nul (0) bij initialisatie'''            
    return df['LAST_MODIFIED'] > df['LAST_TRANSFER']

def CopyTo(df) -> DataFrame:  
    ''' kopieert files van `source` SOURCE_PATH + SOURCE_FILE\n
            naar `destination` DESTINATION_PATH\n
            met `naam` DESTINATION_FILE
            
        Filter
        ------ 
        files die recent gewijzigd zijn `RECENT_FILE_UPDATE` = True\n  
        `ERROR_DESTINATION` & `ERROR_SOURCE` = False\n  
        Return
        ------
        DataFrame met `LAST_TRANSFER` unixtimestamp = `LAST_MODIFIED`\n
            & `RECENT_FILE_UPDATE` bool -> `RecentUpdated()`'''
    try:
        # Filter: geen error in source of destination & file recent aangepast
        df_recent_file_update = df[(df['ERROR_SOURCE'] == 0) & (df['ERROR_DESTINATION'] == 0) & (df['RECENT_FILE_UPDATE'] == 1)]
        df_recent_file_update.apply(lambda x: shutil.copy2(x['SOURCE'], x['DESTINATION']),axis = 1)
        log(df_recent_file_update) # To do
        df['LAST_TRANSFER'] = df['LAST_MODIFIED'] # hier voor alle records gedaan, niet per se fout, maar kan beter ?
        df['RECENT_FILE_UPDATE'] = RecentUpdated(df)
        
    except:
        print('CopyTo error') # error handling TD
        for n, row in df.iterrows():            
            try:
                src = df['SOURCE'][n]
                dest = df['DESTINATION'][n]
                shutil.copy2(src, dest)
                df['LAST_TRANSFER'].iloc[n] = df['LAST_MODIFIED'].iloc[n]
            except:
                print('CopyTo double error') #fix fix
    return df

def log(df):
    pass

if __name__ == "__main__":
    # initialiseer na wachtperiode STARTUP_WAIT
    time.sleep(STARTUP_WAIT)
    SOURCE_inputfile, STARTUP_DF_NoError = __init__()  

    while True:
        time.sleep(sleep_time)
        counter += 1
        print(counter)
        # check every x seconds if ERROR_SOURCE is still accurate TD
        if counter == counter_max:
            STARTUP_DF_NoError = re__init__(SOURCE_inputfile, STARTUP_DF_NoError)
            print('reinit')
            counter = 0

        # kijk voor updates in LAST_MODIFIED bij ERROR(_LIJST) = False
        STATUS_DF_NoError = Get_Status_Wrapper(STARTUP_DF_NoError)
        STATUS_DF_NoError['RECENT_FILE_UPDATE'] = RecentUpdated(STATUS_DF_NoError)
        STATUS_DF_NoError = CopyTo(STATUS_DF_NoError)
