from PIL import Image, ImageOps, ImageEnhance
from langchain_core.prompts.chat import ChatPromptTemplate
import pytesseract
import unicodedata

img = Image.open("page-4@3x.png")

# 1) Preprocess nhẹ
img = img.convert("L")  # grayscale
img = ImageOps.autocontrast(img)
img = ImageEnhance.Contrast(img).enhance(1.8)

# Nếu ảnh nhỏ/mờ: upscale trước khi threshold
scale = 2
img = img.resize((img.width * scale, img.height * scale), Image.Resampling.LANCZOS)

# Threshold đơn giản
img = img.point(lambda p: 255 if p > 170 else 0)

# 2) OCR config cho tiếng Việt
config = "--oem 1 --psm 6 -c preserve_interword_spaces=1"
text = pytesseract.image_to_string(img, lang="vie+eng", config=config)

# 3) Normalize Unicode để dấu tiếng Việt “đúng dạng”
text = unicodedata.normalize("NFC", text)

from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
# from langchain.c


import os
import dotenv


dotenv.load_dotenv()



model_name = os.getenv("LLM_CHAT_MODEL")
api_key = os.getenv("GCP_PROJECT_3")
temperature = 0

llm = ChatGoogleGenerativeAI(
        model=model_name,
        api_key=api_key,
        temperature=temperature,
        max_tokens=None,
        timeout=300,
        max_retries=0,
    )


prompt = ChatPromptTemplate.from_messages([
    ("system", """
            format text to json

        """
    ),
    ("human", "{input}"),
])
# print(llm.invoke(prompt.format(input=text)))
# print(result)
result = llm.invoke(prompt.format(input=text))
print(result.content)
