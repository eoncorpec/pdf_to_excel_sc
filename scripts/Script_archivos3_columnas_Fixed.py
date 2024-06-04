import sys
sys.path.append("../")

ROUTE = "../Inputs/"

import os
allFilesInDirectory = os.listdir(ROUTE)

import pandas as pd
import numpy as np
import pdfplumber
import re
import sqlite3
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



dataPDF = pd.DataFrame()
sqliteFile = "../Outputs/databaseFile.db"
pos: int = 0


validFiles = [
    "ESTADO DE FLUJO EFECTIVO POR EL MÉTODO DIRECTO",        # SALDOSBALANCE EN USD 0
    "ESTADO DE SITUACIÓN FINANCIERA",                        # VALOR EN USD         1
    "ESTADO DE RESULTADO INTEGRAL"                           # VALOR EN USD         2
]


generalColumns = ["CUENTA",
                "VALOR EN USD",
                "SALDOSBALANCE EN USD"]


originalColumnsToReplace = [
                "SALDOS BALANCE (En USD$)",
                "VALOR (En USD$)"
]


tableNames = [
    "estado_flujo",                                         # 0
    "estado_financiera",                                    # 1
    "estado_integral"                                       # 2
]

fileType = ""

def flattenedData(matrix):
    newList = []
    for row in matrix:
        newList.append(row)
    return newList

try:
    data = []
    for document in usefulFiles:
        pdf = pdfplumber.open(document)
        #extract all pages info into one list
        for page in pdf.pages:
            data.extend(page.extract_tables(table_settings={
                "intersection_tolerance":5,
                "intersection_x_tolerance":5,
                "intersection_y_tolerance":5,})
            )

        #remove empty columns and None
        for k in range(len(data)):
            for i in range(len(data[k])):
                if None in data[k][i]:
                    while None in data[k][i]:
                        data[k][i].remove(None)

                if "" in data[k][i]:
                    while "" in data[k][i]:
                        data[k][i].remove("")

        #flatten the original data because it has 3 dimensions
        data = flattenedData(data)


        #this df holds all data for a given document
        Data = pd.DataFrame(data)
        Data.replace(re.compile(r'\r'),"", inplace=True)
        Data.replace(re.compile(r'\n'),"", inplace=True)


        #remove None and "" in rows
        Data.replace(to_replace=[None], value=np.NaN, inplace=True)
        Data.replace(to_replace=[""], value=np.NaN, inplace=True)


        #get the title of the current document
        pos = find_position_of_value_in_column(Data,"CUENTA")
        cols = Data.columns.to_list()

        rowStartTitle = Data.index[ Data[cols[pos]]=="CUENTA" ].to_list()
        tableTitle = Data.iloc[ rowStartTitle[0]-1: rowStartTitle[0] ]
        tableTitle.reset_index(drop=True, inplace=True)
        
        titulo = tableTitle.iloc[0].to_list()[0]

        pos = validFiles.index(titulo)
        fileType = validFiles[pos]


        #slice Data so we get Header
        rowStartTitle = Data.index[ Data[cols[pos]]==fileType ]
        Header = Data.iloc[:rowStartTitle[0]].copy()


        #remove nan and trailing number in the columns of Header
        Header = remove_columns_full_of_Nan(Header, Header.columns.to_list())
        Header.columns = [ str(x) for x in Header.columns.to_list() ]
        Header = remove_trailing_number_in_column(Header)


        #slice Data so we ommit the Header
        Data = Data.iloc[ rowStartTitle[0]+2: ]
        Data.reset_index(drop=True, inplace=True)


        #drop column with código
        listaColumnas = Data.columns.to_list()
        Data.drop(listaColumnas[1], axis=1, inplace=True)

        #set other flags
        if pos == 1 or pos == 2:
            Data.columns = [generalColumns[0], generalColumns[1]]
        else: 
            Data.columns = [generalColumns[0], generalColumns[2]]

        listaColumnas.clear()
        listaColumnas = Data.columns.to_list()

        columnaOriginal = ""
        if pos == 1 or pos == 2:
            columnaOriginal = originalColumnsToReplace[1]
        else:
            columnaOriginal = originalColumnsToReplace[0]

        
        #delete rows with cuenta or (valor or saldos balance) en usd
        indexCodValor = Data[ ( Data[listaColumnas[0]]==listaColumnas[0] ) &
            ( Data[listaColumnas[1]]==columnaOriginal )
        ].index
        Data.drop(indexCodValor, inplace=True)
        Data.reset_index(drop=True, inplace=True)


        #delete rows with the title of the documnent
        indexTitle = Data[ Data[listaColumnas[0]]==fileType ].index
        Data.drop(indexTitle, inplace=True)
        Data.reset_index(drop=True, inplace=True)


        #drop null as string and completely empty rows
        patternDelNull = r"ull"
        filterNullStrings = Data[ Data[listaColumnas[0]].str.contains(patternDelNull, na=False) ].index
        Data.drop(filterNullStrings, inplace=True)

        filterNullStrings = Data[ ( Data[listaColumnas[0]].isna() ) &
                    ( Data[listaColumnas[1]].isna() ) ].index
        Data.drop(filterNullStrings, inplace=True)
        Data.reset_index(drop=True, inplace=True)


        listDfContador = Data[ Data.columns.to_list()[0] ].to_list()


        ColumnasContador  = ["CONTADOR","IDENT. CONTADOR"]
        ColumnasRepresentante = ["REPRESENTANTE LEGAL","IDENT. REPRESENTANTE LEGAL"]


        #check if there's a 'table' with representante legal
        if "REPRESENTANTE(S) LEGAL(ES)" not in listDfContador:
            indiceContadorRow = listDfContador.index("CONTADOR")
            Data.loc[indiceContadorRow] = [" ", np.NaN]
            Data.loc[indiceContadorRow] = [" ", np.NaN]
            Data.loc[indiceContadorRow] = ["REPRESENTANTE(S) LEGAL(ES)", np.NaN]

            listDfContador.insert(indiceContadorRow, " ")
            listDfContador.insert(indiceContadorRow, " ")
            listDfContador.insert(indiceContadorRow, "REPRESENTANTE(S) LEGAL(ES)")
            Data.reset_index(drop=True, inplace=True)

        rowEndData = Data.index[
            Data[Data.columns.to_list()[0]]=="REPRESENTANTE(S) LEGAL(ES)"
        ].to_list()

        Data = Data.iloc[ :rowEndData[0] ]
        Data.reset_index(drop=True, inplace=True)


        #create Dataframe with datos from contador and transpose it
        indexRepLegal = listDfContador.index( "REPRESENTANTE(S) LEGAL(ES)" )
        dfContador = pd.DataFrame( [listDfContador[indexRepLegal:]] )
        dfContador = dfContador.set_index(dfContador.columns.to_list()[0]).T 
        dfContador = remove_columns_full_of_Nan(dfContador, dfContador.columns.to_list())


        #from dfContador only get useful data
        datosRepresentante = []
        datosContador = []
        valoresDfContrReprs = dfContador[ dfContador.columns.to_list()[0] ].to_list()
        index = 0

        while valoresDfContrReprs[index] != "CONTADOR":
            datosRepresentante.append(valoresDfContrReprs[index])
            index += 1

        index += 1

        for k in valoresDfContrReprs[index:]:
            datosContador.append(k)

        if len(datosContador) > 2:
            datosContador.pop(-1)

        if len(datosRepresentante)>2:
            ColumnasRepresentante.extend(ColumnasRepresentante)


        #group all rep-contador columnas and rep-contador data
        ColumnasRepresentante.extend(ColumnasContador)
        datosRepresentante.extend(datosContador)

        datosRepresentante = datosRepresentante[0:len(ColumnasRepresentante)]


        #if necessary insert empty  string
        if len(datosRepresentante) < len(ColumnasRepresentante):
            while( len(datosRepresentante) < len(ColumnasRepresentante) ):
                datosRepresentante.append("")

        
        #update dfContador and create its transposed version
        dfContador = pd.DataFrame([datosRepresentante], columns=ColumnasRepresentante)
        dfContador = rename_duplicate_columns(dfContador)


        #mix dfCOntador and transposed header
        transposedHeader = Header.set_index( Header.columns.to_list()[0] ).T

        for k in range( len(dfContador.columns.to_list()) ):
            transposedHeader.insert( k, dfContador.columns.to_list()[k], dfContador.loc[0, dfContador.columns.to_list()[k]] )

        transposedHeader.replace(re.compile(r"(EL REPRESENTANTE.*|SUPERINTENDENCIA.*)", re.IGNORECASE)," ", inplace=True)

        transposedHeader = rename_duplicate_columns(transposedHeader)
        transposedHeader = remove_trailing_number_in_column(transposedHeader)
        transposedHeader.columns=[
            remove_accents_and_replace(col) for col in transposedHeader.columns
        ]

        
        transposedData = Data.set_index(Data.columns.to_list()[0]).T
        transposedData.columns=[
            remove_accents_and_replace(col) for col in transposedData.columns
        ]
        transposedData = rename_duplicate_columns(transposedData)
        transposedData = remove_trailing_number_in_column(transposedData)


        transposedData.reset_index(drop=True, inplace=True)
        transposedHeader.reset_index(drop=True, inplace=True)


        #insert expediente and anio columns in transposedData
        columnasTransposedHeader = transposedHeader.columns.to_list()
        expAnioIndex = columnasTransposedHeader.index("expediente")
        transposedData.insert(0, "expediente", transposedHeader.loc[0, columnasTransposedHeader[expAnioIndex]])
        expAnioIndex = columnasTransposedHeader.index("anio")
        transposedData.insert(0, "anio", transposedHeader.loc[0, columnasTransposedHeader[expAnioIndex]])

        data.clear()

        #sqlite file
        conn = sqlite3.connect(sqliteFile)
        cursor = conn.cursor()

        chosenTable:str = ""

        chosenTable = tableNames[pos]

        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{chosenTable}'")
        table_exists = cursor.fetchone()

        if not table_exists:
            cursor.execute(f"CREATE TABLE {chosenTable} (id INTEGER PRIMARY KEY)")



        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cabecera'")
        table_exists = cursor.fetchone()
        if not table_exists:
            cursor.execute("CREATE TABLE cabecera (id INTEGER PRIMARY KEY)")


        cursor.execute("PRAGMA table_info(cabecera)")
        existing_columns = [column[1] for column in cursor.fetchall()]
        new_columns = [column for column in transposedHeader.columns if column not in existing_columns]

        for column in new_columns:
            cursor.execute(f"ALTER TABLE cabecera ADD COLUMN {column} NULL")


        cursor.execute(f"PRAGMA table_info({chosenTable})")
        existing_columns = [column[1] for column in cursor.fetchall()]
        new_columns = [column for column in transposedData.columns if column not in existing_columns]

        for column in new_columns:
            cursor.execute(f"ALTER TABLE {chosenTable} ADD COLUMN {column} NULL")

            
        conn.commit()

        transposedData.to_sql(chosenTable, conn, if_exists="append", index=False)
        transposedHeader.to_sql("cabecera", conn, if_exists="append", index=False)
        conn.close()
except Exception as e:
    print(f"Error: {e}")
    print(f"Archivo: {document}")