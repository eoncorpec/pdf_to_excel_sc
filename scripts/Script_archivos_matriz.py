import sys
sys.path.append("../")

ROUTE = "../Inputs/"

import os
allFilesInDirectory = os.listdir(ROUTE)

import pandas as pd
import numpy as np
import re
import sqlite3
import pdfplumber
from Functions.refactor_dataframe_columns import *

pdf_code = sys.argv[1]

#get all files in Inputs directory
#and filter by its code
usefulFiles = []
for currFile in allFilesInDirectory:
    if currFile.__contains__(pdf_code):
        usefulFiles.append(currFile)


#add the complete path to those files
for k in range( len(usefulFiles) ):
    usefulFiles[k] = ROUTE + usefulFiles[k]


sqliteFile = "../Outputs/databaseFile.db"
tableName = "estado_cam_patrimo"



try:
    for document in usefulFiles:
        pdf = pdfplumber.open(document)
        p0 = pdf.pages[0]

        table = p0.extract_table()
        header = table[:6]
        table = table[7:]

        #helpers in case the file has another format eg. an extra row
        columnsHeaders = []
        sampleColumnsNumeric = ['301', '302', '303', '30401', '30402']

        #find the row with those values
        columnsHeaders = p0.within_bbox((10,185, p0.width, 205))
        columnsHeaders = columnsHeaders.extract_text()
        columnsHeaders = columnsHeaders.split(" ")

        #if the list does not have thos numeric values, the file has another format
        #and try this
        if not( set(sampleColumnsNumeric).intersection(set(columnsHeaders)) == set(sampleColumnsNumeric) ):
            for k in table:
                if set(sampleColumnsNumeric).intersection(set(k)) == set(sampleColumnsNumeric):
                    columnsHeaders = k
            
            while None in columnsHeaders:
                columnsHeaders.remove(None)

        columnsHeaders.insert(0,"CODIGO")
        columnsHeaders.insert(0,"CUENTA")
        columnsHeaders.append("TOTAL_PATRIMONIO")


        #create a df with the header info
        dataHeader = pd.DataFrame(header)
        dataHeader.fillna(np.NaN, inplace=True)
        dataHeader = remove_columns_full_of_Nan(dataHeader, dataHeader.columns.to_list())
        dataHeader.drop([dataHeader.columns.to_list()[0]], axis=1, inplace=True)


        #create a df with table info and drop escape characters
        dataInfo = pd.DataFrame(table)
        dataInfo.replace(re.compile(r'\r'),"", inplace=True)
        dataInfo.replace(re.compile(r'\n'),"", inplace=True)


        #slice dataframe since the first row with data
        flagValue: str = "SALDO AL FINAL DELPERÍODO"
        pos = find_position_of_value_in_column(dataInfo, flagValue)
        rowIndex = dataInfo.index[ dataInfo[dataInfo.columns.to_list()[pos]] == flagValue ].to_list()
        dataInfo = dataInfo.iloc[rowIndex[0]: ]
        dataInfo.reset_index(drop=True, inplace=True)

        dataInfo.replace( to_replace=[None], value=np.nan, inplace=True )
        dataInfo = remove_columns_full_of_Nan(dataInfo, dataInfo.columns.to_list())
        dataInfo.columns = columnsHeaders

        #SECOND (LAST) PAGE

        p0 = pdf.pages[1]
        tableP1 = p0.extract_tables()

        #split data into generalData and contador-representante legal
        datosFrameP1 = pd.DataFrame()
        datosContadorRepresentante = pd.DataFrame()
        for k in range(len(tableP1)):
            if k == 0:
                datosFrameP1 = pd.DataFrame(tableP1[k])
            else:
                datosContadorRepresentante = pd.concat([datosContadorRepresentante, pd.DataFrame(tableP1[k])])

        datosFrameP1.replace(re.compile(r'\r'),"",inplace=True)
        datosFrameP1.replace(re.compile(r'\n'),"",inplace=True)

        #forget header end get data
        datosFrameP1 = datosFrameP1.iloc[4:]

        datosFrameP1.reset_index(drop=True, inplace=True)
        datosContadorRepresentante.reset_index(drop=True, inplace=True)

        flagValue = "REALIZACIÓN DE LARESERVA PORVALUACIÓN DE"
        pos = find_position_of_value_in_column(datosFrameP1, flagValue)

        rowIndex = datosFrameP1.index[ datosFrameP1[datosFrameP1.columns.to_list()[pos]] == flagValue ].to_list()
        datosFrameP1 = datosFrameP1.iloc[rowIndex[0]:]
        datosFrameP1.reset_index(drop=True, inplace=True)


        columnasDFContRepr = datosContadorRepresentante.columns.to_list()
        valoresDFContRepr = datosContadorRepresentante[columnasDFContRepr[0]].to_list()
        #if there is not data from representantes legales add to empty rows and REPRESENTANTE(S) LEGAL(ES) as a row
        if "REPRESENTANTE(S) LEGAL(ES)" not in valoresDFContRepr:
            for k in range(2):
                rowToAdd = {columnasDFContRepr[0]: " "}
                datosContadorRepresentante.loc[-1] = rowToAdd
                datosContadorRepresentante.index = datosContadorRepresentante.index+1
                datosContadorRepresentante.sort_index(inplace=True)
                datosContadorRepresentante.reset_index(drop=True, inplace=True)
            rowToAdd = {columnasDFContRepr[0]:"REPRESENTANTE(S) LEGAL(ES)"}
            datosContadorRepresentante.loc[-1] = rowToAdd
            datosContadorRepresentante.index = datosContadorRepresentante.index+1
            datosContadorRepresentante.sort_index(inplace=True)
            datosContadorRepresentante.reset_index(drop=True, inplace=True) 


        ColumnasContador = ["CONTADOR","IDENT. CONTADOR"]
        ColumnasRepresentante = ["REPRESENTANTE LEGAL","IDENT. REPRESENTANTE LEGAL"]
        datosContadorRepresentante.replace("null", " ", inplace=True)

        datosFrameP1.columns = columnsHeaders
        dataFrameValores = pd.concat([dataInfo, datosFrameP1], ignore_index=True)


        datosRepresentante =[]
        datosContador =[]
        valoresDFContRepr=datosContadorRepresentante[columnasDFContRepr[0]].to_list()
        index = 1
        while valoresDFContRepr[index] != "CONTADOR":
            datosRepresentante.append(valoresDFContRepr[index])
            index += 1

        index += 1
        for k in valoresDFContRepr[index:]:
            datosContador.append(k)

        if len(datosContador) > 3:
            datosContador.pop(-1)
            datosContador.pop(-1)

        lenReps = len(datosRepresentante)
        for k in range(lenReps):
            if k%2 == 0 and k != 0:
                ColumnasRepresentante.extend(ColumnasRepresentante)

        lenConts = len(datosContador)
        for k in range(lenConts):
            if k%3 == 0 and k != 0:
                ColumnasContador.extend(ColumnasContador)

        ColumnasRepresentante.extend(ColumnasContador)
        datosRepresentante.extend(datosContador)

        if "" in datosRepresentante:
            datosRepresentante.remove("")

        if len(datosRepresentante) %2 != 0:
            datosRepresentante.pop(-1)

        dfContador = pd.DataFrame([datosRepresentante], columns=ColumnasRepresentante)
        dfContador = rename_duplicate_columns(dfContador)

        #convert the header into a 'row' and add the values of the contador dataFrame
        transposedHeader = dataHeader.set_index(
            dataHeader.columns.to_list()[0] ).T
        for k in range(len( dfContador.columns.to_list() )):
            transposedHeader.insert(k, dfContador.columns.to_list()[k], dfContador.loc[0,dfContador.columns.to_list()[k]])



        transposedHeader.columns = [
            remove_accents_and_replace(col) for col in transposedHeader.columns
        ]

        dataFrameValores.colums = [
            remove_accents_and_replace(col) for col in dataFrameValores.columns
        ]
        dataFrameValores.columns = [str(val) for val in dataFrameValores.columns.to_list()]
        dataFrameValores = rename_duplicate_columns(dataFrameValores)
        dataFrameValores = remove_trailing_number_in_column(dataFrameValores)
        dataFrameValores.reset_index(drop=True, inplace=True)
        

        columnasTransposedHeader = transposedHeader.columns.to_list()
        
        expAnioIndex = columnasTransposedHeader.index("expediente")
        dataFrameValores.insert(0,"expediente", transposedHeader.loc[dataHeader.columns.to_list()[1], columnasTransposedHeader[expAnioIndex]])
        expAnioIndex = columnasTransposedHeader.index("anio")
        dataFrameValores.insert(0,"anio", transposedHeader.loc[dataHeader.columns.to_list()[1], columnasTransposedHeader[expAnioIndex]])
        
        pdf.close()


        conn = sqlite3.connect(sqliteFile)
        cursor = conn.cursor()

        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tableName}'")
        table_exists = cursor.fetchone()
        if not table_exists:
            cursor.execute(f"CREATE TABLE {tableName} (id INTEGER PRIMARY KEY AUTOINCREMENT)")


        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cabecera'")
        table_exists = cursor.fetchone()
        if not table_exists:
            cursor.execute("CREATE TABLE cabecera (id INTEGER PRIMARY KEY)")


        cursor.execute("PRAGMA table_info(cabecera)")
        existing_columns = [column[1] for column in cursor.fetchall()]
        new_columns = [column for column in transposedHeader.columns if column not in existing_columns]

        for column in new_columns:
            cursor.execute(f"ALTER TABLE cabecera ADD COLUMN {column} NULL")

        
        cursor.execute(f"PRAGMA table_info({tableName})")
        existing_columns = [column[1] for column in cursor.fetchall()]
        new_columns = [ column for column in  dataFrameValores.columns if column not in existing_columns]

        for column in new_columns:
            cursor.execute(f"ALTER TABLE {tableName} ADD COLUMN {column} NULL")
        
        conn.commit()
        
        transposedHeader.to_sql("cabecera", conn, if_exists="append", index=False)
        dataFrameValores.to_sql(tableName, conn, if_exists="append", index=False)
        conn.close()
except Exception as e:
    print(f"Error: {e}")
    print(f"Error: {document}")