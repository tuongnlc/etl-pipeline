import cv2
from PIL import Image
import pytesseract
import unicodedata

img_path = "hpg/page-30.png"
rotate_deg = 0

bgr = cv2.imread(img_path)
if bgr is None:
    raise FileNotFoundError(img_path)

if rotate_deg:
    deg = rotate_deg % 360
    if deg == 90:
        bgr = cv2.rotate(bgr, cv2.ROTATE_90_CLOCKWISE)
    elif deg == 180:
        bgr = cv2.rotate(bgr, cv2.ROTATE_180)
    elif deg == 270:
        bgr = cv2.rotate(bgr, cv2.ROTATE_90_COUNTERCLOCKWISE)
    else:
        h0, w0 = bgr.shape[:2]
        c = (w0 / 2.0, h0 / 2.0)
        m = cv2.getRotationMatrix2D(c, rotate_deg, 1.0)
        cos = abs(m[0, 0])
        sin = abs(m[0, 1])
        w1 = int((h0 * sin) + (w0 * cos))
        h1 = int((h0 * cos) + (w0 * sin))
        m[0, 2] += (w1 / 2.0) - c[0]
        m[1, 2] += (h1 / 2.0) - c[1]
        bgr = cv2.warpAffine(
            bgr,
            m,
            (w1, h1),
            flags=cv2.INTER_CUBIC,
            borderMode=cv2.BORDER_REPLICATE,
        )

scale = 2
bgr = cv2.resize(
    bgr,
    (bgr.shape[1] * scale, bgr.shape[0] * scale),
    interpolation=cv2.INTER_CUBIC,
)

gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
gray = cv2.GaussianBlur(gray, (3, 3), 0)

bin_inv = cv2.adaptiveThreshold(
    gray,
    255,
    cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
    cv2.THRESH_BINARY_INV,
    41,
    15,
)

h, w = bin_inv.shape[:2]

h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (max(60, w // 25), 1))
v_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, max(60, h // 25)))

h_lines = cv2.morphologyEx(bin_inv, cv2.MORPH_OPEN, h_kernel, iterations=2)
v_lines = cv2.morphologyEx(bin_inv, cv2.MORPH_OPEN, v_kernel, iterations=2)
lines = cv2.bitwise_or(h_lines, v_lines)
lines = cv2.dilate(lines, cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)), iterations=1)

clean = cv2.inpaint(gray, lines, 3, cv2.INPAINT_TELEA)

clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
clean = clahe.apply(clean)

pil = Image.fromarray(clean)

config = "--oem 1 --psm 3 --dpi 300 -c preserve_interword_spaces=1"
text = pytesseract.image_to_string(pil, lang="vie+eng", config=config)
text = unicodedata.normalize("NFC", text)

print(text)