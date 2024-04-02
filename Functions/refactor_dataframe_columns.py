import pandas as pd
import re

def remove_accents_and_replace(text):
    text = text.strip()
    text = text.lower()
    text = text.replace("(-) ", "")
    text = (
        text.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )
    text = text.replace("año", "anio")
    text = text.replace("ü", "u").replace("ñ", "n").replace("ç", "c")
    text = text.replace(" ", "_")
    text = re.sub(r"\W+", "", text)
    return text


def rename_duplicate_columns(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [
            dup + "_" + str(i) if i != 0 else dup for i in range(sum(cols == dup))
        ]
    df.columns = cols
    return df

def remove_trailing_number_in_column(df):
    cols = df.columns.to_list()
    for k in range(len(cols)):
        if cols[k][0].isdecimal():
            cols[k] = "_"+cols[k]
    
    df.columns = cols
    return df


def remove_columns_full_of_Nan(df, listaColumnasAllData):
    contadorNulls = 0
    for k in listaColumnasAllData:
        columnValues = df[k].to_list()

        for cell in columnValues:
            if pd.isna(cell):
                contadorNulls += 1

        if contadorNulls == len(df):
            df.drop([k], axis=1, inplace=True)
        
        contadorNulls = 0
        columnValues.clear()
    return df
