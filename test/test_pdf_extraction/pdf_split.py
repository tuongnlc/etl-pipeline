import pymupdf
from pathlib import Path

pdf_file_name = "hpg/hpg.pdf"
out_dir = Path("hpg")
out_dir.mkdir(parents=True, exist_ok=True)

dpi = 300

with pymupdf.open(pdf_file_name) as doc:
    for page_index in range(len(doc)):
        page = doc[page_index]
        try:
            pix = page.get_pixmap(dpi=dpi, alpha=False)
        except TypeError:
            # zoom = dpi / 72
            zoom=6
            mat = pymupdf.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat, alpha=False)

        pix.save(str(out_dir / f"page-{page_index}.png"))