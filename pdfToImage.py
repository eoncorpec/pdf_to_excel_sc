from pdf2image import convert_from_path
import os

pdf_path: str = "./Inputs/DocEconomica_731956_3_1_1_2020_11_04_19_44_42_860.pdf"

outputDirectory = "./Images/Doc1"

if not os.path.exists(outputDirectory):
    os.makedirs(outputDirectory)

images = convert_from_path(pdf_path)


for i, image in enumerate(images):
    image_path = os.path.join(outputDirectory, f"page{i}.jpg")
    image.save(image_path,"JPEG")

"""
for k in range(len(images)):
    images[k].save("page"+str(k)+".jpg", 'JPEG')
"""