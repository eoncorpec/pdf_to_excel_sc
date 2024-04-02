#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import tabula
import re
import sqlite3
from tqdm import tqdm


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


# Function to rename columns with duplicate names
def rename_duplicate_columns(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [
            dup + "_" + str(i) if i != 0 else dup for i in range(sum(cols == dup))
        ]
    df.columns = cols
    return df


files = [
    "../Inputs/DocEconomica_384_3_1_1_2020_12_31_00_00_00_000.pdf",
    "../Inputs/DocEconomica_384_3_1_1_2021_12_31_00_00_00_000.pdf",
    "../Inputs/DocEconomica_384_3_1_1_2022_12_31_00_00_00_000.pdf",
    "../Inputs/DocEconomica_6712_3_1_1_2020_12_31_00_00_00_000.pdf",
    "../Inputs/DocEconomica_6712_3_1_1_2021_12_31_00_00_00_000.pdf",
    "../Inputs/DocEconomica_6712_3_1_1_2022_12_31_00_00_00_000.pdf",
    "../Inputs/DocEconomica_736008_3_1_1_2021_12_31_00_00_00_000.pdf",
    "../Inputs/DocEconomica_736008_3_1_1_2022_12_31_00_00_00_000.pdf",
]

coords = {
    "1": (126.82, 20.08, 814.79, 574.18),
    "2": (36.08, 20.76, 822.24, 575.60),
    "3": (36.08, 20.76, 822.24, 575.60),
    "4": (36.08, 20.76, 822.24, 575.60),
    "5": (36.08, 20.76, 822.24, 575.60),
    "6": (36.08, 20.76, 822.24, 575.60),
    "7": (36.08, 20.76, 822.24, 575.60),
}
# Activar solo para pruebas
# files = ["../Inputs/DocEconomica_384_3_1_1_2022_12_31_00_00_00_000.pdf"]

conn = sqlite3.connect("../Outputs/datos_empresas.db")
# pdfOriginal = "../Inputs/DocEconomica_384_3_1_1_2022_12_31_00_00_00_000.pdf"
# excel_output = "../Outputs/estados_situacion_financiera.xlsx"
for _, file in tqdm(enumerate(files)):

    dfData = pd.DataFrame()

    length = len(coords)
    try:
        for dfIndex in range(length):
            dfPdf = tabula.read_pdf(
                file,
                pages=(dfIndex + 1),
                area=coords[str(dfIndex + 1)],
                lattice=True,
                output_format="dataframe",
                multiple_tables=True,   
                encoding="latin-1"
            )
            dfData = pd.concat([dfData, dfPdf[0]])
    except Exception as e:
        #print(f"falla {file}")
        print(f"falla {e}")
        continue

    dfData = dfData.astype("object")
    dfData.drop(["CÓDIGO"], axis=1, inplace=True)
    df_transposed = dfData.set_index("CUENTA").T
    df_transposed = df_transposed.reset_index(drop=True)
    df_transposed.insert(0, "Expediente", file.split("_")[1])
    df_transposed.insert(1, "Año", file.split("_")[-7])
    df_transposed.columns = [
        remove_accents_and_replace(col) for col in df_transposed.columns
    ]
    df = rename_duplicate_columns(df_transposed)
    df.to_sql("datos_super", conn, if_exists="append", index=False)
