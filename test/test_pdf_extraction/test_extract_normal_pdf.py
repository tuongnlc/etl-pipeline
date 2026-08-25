#brew install tesseract
#brew install tesseract-lang-eng
from PIL import Image
import pymupdf

doc = pymupdf.open("hpg/hpg.pdf")
page = doc[1]
print(type(page))
tables = page.get_text()
print(tables)
# print(page.get_textbox(rectangle=None))

# import pymupdf

# with pymupdf.open("vnm_3/vnm_3.pdf") as doc:
#     page = doc[1]

#     finder = page.find_tables()
#     print("num_tables =", len(finder.tables))

#     for ti, table in enumerate(finder.tables):
#         print(f"\n=== TABLE {ti} bbox={table.bbox} ===")

#         rows = table.extract()  # list[list[str|None]]
#         for row in rows:
#             row = [(c or "").strip() for c in row]
#             print("\t".join(row))