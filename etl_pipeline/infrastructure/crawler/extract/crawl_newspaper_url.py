from urllib.parse import urljoin
import re
import httpx
from bs4 import BeautifulSoup
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from etl_pipeline.templates.etl.extract.crawler_extractor import CrawlerExtractor


class CrawlNewspaperUrl(CrawlerExtractor):
    # Pattern (regex) của những link cần bỏ qua (điều hướng, chuyên mục, quảng cáo...)
    IGNORE_PATTERNS = (
        r"/$",                                   # URL kết thúc bằng "/" -> trang chuyên mục
        r"^https?://finance\.vietstock\.vn/",    # subdomain không phải bài viết
        r"/doanh-nghiep-a-z/?$",
        # cafef.vn: bài viết luôn có ID số ở cuối (VD ...-188261105150235851.chn).
        # -> bỏ mọi URL cafef KHÔNG có ID số (chuyên mục, tag, tìm kiếm...),
        #    VD: https://cafef.vn/thi-truong-chung-khoan.chn
        r"^https?://(?:[^/]+\.)?cafef\.vn/(?!.*-\d{10,}\.chn$)",
        # thoibaotaichinhvietnam.vn: bài viết kết thúc bằng ID số + .html (VD ...-206463.html).
        # -> bỏ mọi URL thoibao KHÔNG có ID số, VD: https://thoibaotaichinhvietnam.vn/chung-khoan
        r"^https?://(?:[^/]+\.)?thoibaotaichinhvietnam\.vn/(?!.*-\d{3,}\.html$)",
    )

    # Schema của DataFrame links (gom 2 cột: text, url).
    SCHEMA = StructType([
        StructField("text", StringType(), True),
        StructField("url", StringType(), True),
    ])

    def __init__(self, link: str, spark: SparkSession) -> None:
        self.link = link
        self.spark = spark

    def _should_ignore(self, url: str) -> bool:
        return any(re.search(pattern, url) for pattern in self.IGNORE_PATTERNS)

    def extract(self) -> DataFrame:
        with httpx.Client(
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
        ) as client:
            resp = client.get(self.link)
            resp.raise_for_status()
            html = resp.text

        soup = BeautifulSoup(html, "lxml")
        links = []
        for a in soup.select("h2 a, h3 a, h4 a"):
            href = a.get("href")
            if not href:
                continue

            url = urljoin(str(resp.url), href)
            if self._should_ignore(url):
                continue

            links.append({
                "text": a.get_text(strip=True),
                "url": url,
            })

        # Build spark dataframe
        df = self.spark.createDataFrame(links, schema=self.SCHEMA)
        return df
