import cv2
import pytesseract
import pandas as pd

pytesseract.pytesseract.tesseract_cmd = r'C:/Program Files/Tesseract-OCR/tesseract'

image = cv2.imread("./Images/Doc1/page0.jpg")
gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
_, threshold_image= cv2.threshold(gray_image, 0,255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
myconfig = r"--psm 3 --oem 3"
imageString = pytesseract.image_to_string(threshold_image, config=myconfig)
#print(f"Info: {imageString}")

rows = imageString.split("\n")
table_data = [row.split("\t") for row in rows]
for row in table_data:
    for i, cell in enumerate(row):
        row[i] = ''.join(char for char in cell if ord(char) < 128)
 
df = pd.DataFrame(table_data)
df = df.applymap(lambda x: x if x.strip() != "" else pd.NA)
output_excel_file = "./Outputs/Doc1/sheet.xlsx"
df.to_excel(output_excel_file, index=False, header=False)


"""
columna_superior_1 = {"Campo": [], "Valor": []}
columna_superior_2 = {"Campo": [], "Valor": []}
table_data = {"Cuenta": [], "Código": [], "Valor (USD)": []}

procesando_tabla = False

for line in lines:
    if "ESTADO DE SITUACION FINANCIERA" in line:
        procesando_tabla = True
        continue

    if not procesando_tabla:
        words = line.split(':', 1)

        # Verifica si hay suficientes palabras para representar una fila de la tabla
        if len(words) == 2:
            # Asigna cada palabra a la columna correspondiente
            columna_superior_1["Campo"].append(words[0].strip())
            columna_superior_1["Valor"].append(words[1].strip())

    else:
        # Divide la línea en palabras
        words = line.split()

        # Verifica si hay suficientes palabras para representar una fila de la tabla
        if len(words) == 3:
            # Asigna cada palabra a la columna correspondiente
            table_data["Cuenta"].append(words[0])
            table_data["Código"].append(words[1])
            table_data["Valor (USD)"].append(words[2])

df_columna_superior_1 = pd.DataFrame(columna_superior_1)
df_columna_superior_2 = pd.DataFrame(columna_superior_2)
df_tabla_data = pd.DataFrame(table_data)

# Guarda los DataFrames en un archivo Excel
with pd.ExcelWriter('./output.xlsx') as writer:
    df_columna_superior_1.to_excel(writer, sheet_name='Columna Superior 1', index=False)
    df_columna_superior_2.to_excel(writer, sheet_name='Columna Superior 2', index=False)
    df_tabla_data.to_excel(writer, sheet_name='Tabla Data', index=False)
"""