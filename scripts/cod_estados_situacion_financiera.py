#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import tabula


# In[2]:


#al ser la primera vez unificando los archivos, el pdf original será el primero,
#luego sobre el se irán acumulando la demás información


# In[3]:


pdfOriginal = "../Inputs/DocEconomica_384_3_1_1_2022_12_31_00_00_00_000.pdf"
excel_output = "../Outputs/estados_situacion_financiera.xlsx"


#  Ahora se vuelve a leer el archivo anterior
#  Se procede a añadir los datos del nuevo pdf

# In[4]:


with pd.ExcelFile(excel_output) as xls:
    # Lee cada hoja por separado
    dfAcumulado_encabezado = pd.read_excel(xls, 'Encabezado', dtype="object")
    dfAcumulado_datps = pd.read_excel(xls, 'Datos', dtype="object")


# In[5]:


dfAcumulado_datps


# In[ ]:





# In[ ]:





# In[6]:


coords = {
    "1":(126.82, 20.08, 814.79, 574.18),
    "2":(36.08, 20.76, 822.24, 575.60),
    "3":(36.08, 20.76, 822.24, 575.60),
    "4":(36.08, 20.76, 822.24, 575.60),
    "5":(36.08, 20.76, 822.24, 575.60),
    "6":(36.08, 20.76, 822.24, 575.60),
    "7":(36.08, 20.76, 822.24, 575.60)
}


# In[7]:


dfData = pd.DataFrame()


# In[8]:


length = len(coords)
for dfIndex in range(length):
    dfPdf = tabula.read_pdf(pdfOriginal, pages=(dfIndex+1), area=coords[str(dfIndex+1)], lattice=True, output_format="dataframe")
    dfData = pd.concat([dfData, dfPdf[0]])


# In[9]:


dfData
dfData = dfData.astype("object")


# In[10]:


dfData.iloc[0:59]


# In[11]:


dfData.tail(20)


# In[12]:


dfData.iloc[59:110]


# In[13]:


dfData.info()


# In[14]:


dfData[dfData["VALOR (En USD$)"].isna()]


# In[15]:


indexCodCuenValor = dfData[ (dfData["CUENTA"]=="CUENTA")
                        &
                        (dfData["CÓDIGO"]=="CÓDIGO") & (dfData["VALOR (En USD$)"]=="VALOR (En USD$)")].index


# In[16]:


indexCodCuenValor


# In[17]:


dfData.drop(indexCodCuenValor, inplace=True)
dfData.info()


# In[18]:


dfData.iloc[0:59]


# In[19]:


dfData.drop(["CÓDIGO"], axis=1, inplace=True)


# In[20]:


dfData.head(10)


# In[21]:


texto = dfData["CUENTA"].to_list()
valores_list = dfData["VALOR (En USD$)"].to_list()


# In[22]:


valores = np.array(valores_list).reshape(1,-1)
datosFrame = pd.DataFrame(valores, columns=texto)


# In[23]:


datosFrame = datosFrame.astype("object")
datosFrame


# In[24]:


Header = tabula.read_pdf( pdfOriginal, pages=1, area=(21.20, 22.24, 115.71, 572.62), lattice=True, output_format="dataframe")
dafHeader = Header[0]
dafHeader


# In[25]:


columns_names = ["REPRESENTANTE LEGAL","CONTADOR"]
pandas_options = {"names":columns_names}
contador = tabula.read_pdf(pdfOriginal, pages=8, area=(50.82, 20.08, 130.79, 574.18), lattice=False, output_format="dataframe", multiple_tables=True, pandas_options=pandas_options)
contador


# In[26]:


dafContador = contador[0]
dafContador


# In[27]:


dafContador.fillna("", inplace=True)
dafContador


# In[28]:


row_expediente = dafHeader.iloc[1].to_dict()
row_anio = dafHeader.iloc[3].to_dict()


# In[29]:


print(row_expediente["RAZÓN SOCIAL"])
print(row_anio)


# In[30]:


dictContador = {row_expediente["RAZÓN SOCIAL"]: row_expediente["CORPORACION FAVORITA C.A."]}
dictAnio = {row_anio["RAZÓN SOCIAL"]: row_anio["CORPORACION FAVORITA C.A."]}
print(dictContador)
print(dictAnio)


# In[31]:


datosFrame.insert(loc=0, column=row_expediente["RAZÓN SOCIAL"], value=row_expediente["CORPORACION FAVORITA C.A."])


# In[32]:


datosFrame


# In[33]:


datosFrame.insert(loc=1, column=row_anio["RAZÓN SOCIAL"], value=row_anio["CORPORACION FAVORITA C.A."])
datosFrame


# In[34]:


datosFrame = datosFrame.astype("object")
datosFrame


# In[35]:


dafHeader


# In[36]:


newRow = pd.DataFrame([dafHeader.columns.to_list()], columns=dafHeader.columns)
newRow


# In[37]:


dataframeCabecera = pd.concat([newRow, dafHeader])
dataframeCabecera


# In[38]:


textosCabecera = dataframeCabecera["RAZÓN SOCIAL"].to_list()
datosCorpCabecera = dataframeCabecera["CORPORACION FAVORITA C.A."].to_list()
print(textosCabecera)
print(datosCorpCabecera)


# In[39]:


dafContador


# In[40]:


dafContador.drop([0], inplace=True)
dafContador


# In[41]:


addedColumnsList = "REPRESENTANTE LEGAL|IDENT. REPRESENTANTE LEGAL|CONTADOR|IDENT. CONTADOR|COD.CONTADOR".split("|")
addedColumnsList
#textosCabecera.extend([])


# In[42]:


textosCabecera.extend(addedColumnsList)
textosCabecera


# In[43]:


dafContador


# In[44]:


row1contador = dafContador.iloc[0].to_dict()
row2replegal = dafContador.iloc[1].to_dict()
row3cedulas = dafContador.iloc[2].to_dict()
row3codcontador = dafContador.iloc[3].to_dict()
print(row1contador)
print(row2replegal)
print(row3cedulas)
print(row3codcontador)


# In[45]:


datosCorpCabecera.extend( [row2replegal["REPRESENTANTE LEGAL"], row3cedulas["REPRESENTANTE LEGAL"], row1contador["CONTADOR"], row3cedulas["CONTADOR"], row3codcontador["CONTADOR"]] )
datosCorpCabecera


# In[46]:


datosCorpCabecera = np.array(datosCorpCabecera).reshape(1,-1)
dataframeCabeceraContador = pd.DataFrame(datosCorpCabecera, columns=textosCabecera)
dataframeCabeceraContador


# In[47]:


datosFrame


# In[48]:


dfAcumulado_datps


# In[ ]:





# In[49]:


data = datosFrame.astype(str)
data


# In[ ]:





# In[50]:


with pd.ExcelWriter(excel_output, engine="openpyxl", if_sheet_exists="overlay", mode="a") as writer:
    dataframeCabeceraContador.to_excel(writer, sheet_name="Encabezado", header=False,index=False, startrow=len(dfAcumulado_encabezado)+1)
    data.to_excel(writer, sheet_name="Datos",header=False ,index=False, startrow=len(dfAcumulado_datps)+1,)


# In[ ]:




