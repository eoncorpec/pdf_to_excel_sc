from pdf2image import convert_from_path
import os

pdf_path= "./Inputs/Docs2LaFavorita/resultado_integral.pdf"

outputDirectory = "./Images/Docs2LaFavorita/ResultadoIntegral_Dir"

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