import cv2
import pytesseract
import pandas as pd

pytesseract.pytesseract.tesseract_cmd = r'C:/Program Files/Tesseract-OCR/tesseract'

df = pd.DataFrame()
table_data = []
i= 1
while i <= 6:
    image = cv2.imread(f"./Images./Doc1/page{i}.jpg")
    gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    _, threshold_image = cv2.threshold(gray_image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    myconfig = r"--psm 1 --oem 3"
    imageString = pytesseract.image_to_string(threshold_image, config=myconfig)

    rows = imageString.split("\n")
    table_data.extend([row.split("\t") for row in rows])
    
    


    i+=1
df = pd.DataFrame(table_data)
df = df.applymap(lambda x: x if x.strip() != "" else pd.NA)
output_excel_file = "./Outputs/Doc1/Improvecontinued.xlsx"
df.to_excel(output_excel_file, index=False, header=False)