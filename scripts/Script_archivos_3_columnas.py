import sys
sys.path.append("../")

ROUTE = "../Inputs/"

import os
allFilesInDirectory = os.listdir(ROUTE)

import pandas as pd
import numpy as np
import tabula
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


try:
    for document in usefulFiles:
        dataPDF = tabula.read_pdf(document, pages="all", multiple_tables=True, lattice=True, output_format="dataframe", encoding="latin-1")[0]

        #find file's title
        pos = find_position_of_value_in_column(dataPDF,"CUENTA")
        cols = dataPDF.columns.to_list()
        rowStartIndex = dataPDF.index[ dataPDF[cols[pos]]=="CUENTA" ].to_list()
        tableTitle = dataPDF.iloc[ rowStartIndex[0]-1: rowStartIndex[0] ]
        tableTitle.reset_index(drop=True, inplace=True)

        #set flags for current file
        titulo = tableTitle.iloc[0].to_list()[0]
        pos = validFiles.index(titulo)
        fileType = validFiles[pos]

        #slice data to have the Header of the table
        listaColumnas = dataPDF.columns.to_list()
        rowStartIndex = dataPDF.index[ dataPDF[listaColumnas[0]]==fileType ].to_list()
        dfHeader = dataPDF.iloc[ : rowStartIndex[0]].copy()

        dataPDF.drop([listaColumnas[1]], axis=1, inplace=True)


        if pos == 1 or pos == 2:
            dataPDF.columns = [generalColumns[0],generalColumns[1]] #dataPDF.columns = ["CUENTA","VALOR EN USD"]
        else:
            #pos == 0
            dataPDF.columns = [generalColumns[0],generalColumns[2]]

        listaColumnas.clear()
        listaColumnas = dataPDF.columns.to_list()


        columnaOriginal = ""
        if pos == 1 or pos == 2:
            columnaOriginal = originalColumnsToReplace[1]
        else:
            #pos == 0
            columnaOriginal = originalColumnsToReplace[0]

        #delete rows where its name is the name of the column
        indexCodValor = dataPDF[ ( dataPDF[listaColumnas[0]]==listaColumnas[0] ) &
                ( dataPDF[listaColumnas[1]]==columnaOriginal )
            ].index
        
        dataPDF.drop(indexCodValor, inplace=True)

        #delete rows with the value null or ull as strings
        patternDelNull = r"ull"
        filterNullStrings = dataPDF[ dataPDF[listaColumnas[0]].str.contains(patternDelNull, na=False) ].index
        dataPDF.drop(filterNullStrings, inplace=True)

        #drop empty rows
        filterNullStrings = dataPDF[ ( dataPDF[listaColumnas[0]].isna() ) &
                ( dataPDF[listaColumnas[1]].isna() ) ].index
        
        dataPDF.drop(filterNullStrings, inplace=True)
        dataPDF.reset_index(drop=True, inplace=True)


        #bring all data since the row with representantes legales
        rowStartIndex = dataPDF.index[ dataPDF[listaColumnas[0]]=="REPRESENTANTE(S) LEGAL(ES)" ].to_list()
        dfContador = dataPDF.iloc[ rowStartIndex[0]: ].copy()
        dfContador.columns = ["REPRESENTANTE(S) LEGAL(ES)","CONTADOR"]

        filterNullStrings = dfContador[ 
            (dfContador[dfContador.columns.to_list()[0]]=="REPRESENTANTE(S) LEGAL(ES)") |
            (dfContador[dfContador.columns.to_list()[0]]=="CONTADOR") ].index
        dfContador.drop(filterNullStrings, inplace=True)


        listaColumnasContador = dfContador.columns.to_list()
        dfContador.reset_index(drop=True, inplace=True)

        pattern = dfContador.loc[0,"REPRESENTANTE(S) LEGAL(ES)"].split("\r")[0]
        similar_rows = dfContador[dfContador[listaColumnasContador[0]].str.contains(pattern, case=False, na=False)]
        if(len(similar_rows) > 1):
            dfContador.drop(similar_rows.index.to_list()[0], inplace=True)


        dfContador.reset_index(drop=True, inplace=True)
        ColumnasContador= ["REPRESENTANTE LEGAL","IDENT. REPRESENTANTE LEGAL","CONTADOR","IDENT. CONTADOR"]

        #only add data that fits the list above into datosContador
        datosContador = []
        if len(dfContador) >= len(ColumnasContador):
            for k in range(len(ColumnasContador)):
                datosContador.append( dfContador.loc[k, listaColumnasContador[0]] )
        else:
            for k in range(len(ColumnasContador)):
                if k in range(len(dfContador)):
                    datosContador.append( dfContador.loc[k, listaColumnasContador[0]] )
                else:
                    datosContador.append(" ")


        #reformat  header dataframe
        headerColumns = dfHeader.columns.to_list()
        dfHeader.drop([headerColumns[-1]], axis=1, inplace=True)
        headerColumns.pop(0)
        dfHeader.columns = headerColumns

        newFirstRow = pd.DataFrame([dfHeader.columns.to_list()], columns=dfHeader.columns)
        dataFrameHeaderContador = pd.concat([newFirstRow, dfHeader])
        dataFrameHeaderContador.reset_index(drop=True, inplace=True)

        #just select rows with data from the title of the document 'til the row with representantes legales 
        rowStartIndex = dataPDF.index[ dataPDF[listaColumnas[0]]==fileType ].to_list()
        dataPDF = dataPDF.iloc[ rowStartIndex[0]: ]
        dataPDF.reset_index(drop=True, inplace=True)

        rowStartIndex = dataPDF.index[ dataPDF[listaColumnas[0]]=="REPRESENTANTE(S) LEGAL(ES)" ].to_list()
        dataPDF = dataPDF.iloc[ : rowStartIndex[0] ]

        #drop empty and rows with the title
        filterNullStrings = dataPDF[ (dataPDF[listaColumnas[0]]==fileType) & 
                (dataPDF[listaColumnas[1]].isna()) ].index
        dataPDF.drop(filterNullStrings, inplace=True)

        #drop rows where both columns are empty
        filterNullStrings = dataPDF[ (dataPDF[listaColumnas[0]].isna()) & 
                (dataPDF[listaColumnas[1]].isna()) ].index
        dataPDF.drop(filterNullStrings, inplace=True)
        dataPDF.reset_index(drop=True, inplace=True)


        if len(datosContador) <= 0:
            for k in range(len(ColumnasContador)):
                datosContador.append(" ")

        dfContador = pd.DataFrame([datosContador], columns =ColumnasContador)
        transposedHeader = dataFrameHeaderContador.set_index( dataFrameHeaderContador.columns.to_list()[0]).T


        for k in range( len(dfContador.columns.to_list()) ):
            transposedHeader.insert(k, dfContador.columns.to_list()[k], dfContador.loc[0, dfContador.columns.to_list()[k]])


        transposedHeader.replace(re.compile(r"(EL REPRESENTANTE.*|SUPERINTENDENCIA.*)", re.IGNORECASE)," ", inplace=True)

        transposedHeader.reset_index(drop=True, inplace=True)
        transposedHeader.columns = [
            remove_accents_and_replace(col) for col in transposedHeader.columns
        ]


        transposedData = dataPDF.set_index( listaColumnas[0] ).T

        transposedData.columns = [
            remove_accents_and_replace(col) for col in transposedData.columns
        ]
        datosTransposed = rename_duplicate_columns(transposedData)

        data = remove_trailing_number_in_column(datosTransposed)

        columnasTransposedHeader = transposedHeader.columns.to_list()
        expAnioIndex = columnasTransposedHeader.index("expediente")
        data.insert( 0,"expediente", transposedHeader.loc[0, columnasTransposedHeader[expAnioIndex]] )
        expAnioIndex = columnasTransposedHeader.index("anio")
        data.insert( 0,"anio", transposedHeader.loc[0, columnasTransposedHeader[expAnioIndex]] )


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
        new_columns = [column for column in data.columns if column not in existing_columns]

        for column in new_columns:
            cursor.execute(f"ALTER TABLE {chosenTable} ADD COLUMN {column} NULL")

            
        conn.commit()

        data.to_sql(chosenTable, conn, if_exists="append", index=False)
        transposedHeader.to_sql("cabecera", conn, if_exists="append", index=False)
        conn.close()
except Exception as e:
    print(f"Error: {e}")
    print(f"Archivo: {document}")