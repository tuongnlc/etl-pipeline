from abc import ABC, abstractmethod
from typing import Any



class BasePipeline(ABC):
    @abstractmethod
    def extract(self) -> None:
        raise NotImplementedError("extract method must be implemented")

    def transform(selfs, df, transformation_steps: list[Any]) -> None:
        """
            Transform the extracted data.
        """
        if transformation_steps is not None:
            for step in transformation_steps:
                df = step.transform(df)
        if df is not None:
            return df
        return None

    @abstractmethod
    def load(self) -> None:
        raise NotImplementedError("load method must be implemented")
        
    def run(self, df, transformation_steps: list[Any]) -> None:
        self.extract()
        df = self.transform(df, transformation_steps)
        self.load()


class BaseCrawlerPipeline(BasePipeline):
    """
    Base class cho các pipeline dạng crawler.

    Thứ tự chạy: extract (optional) -> crawl (required) -> transform (optional) -> load (required)

    - extract:   tuỳ chọn, chuẩn bị dữ liệu đầu vào cho bước crawl (VD: danh sách URL).
    - crawl:     bắt buộc, thu thập dữ liệu.
    - transform: tuỳ chọn, áp dụng các transform step lên dữ liệu đã crawl.
    - load:      bắt buộc, ghi dữ liệu ra đích.
    """

    def extract(self) -> Any:
        """
        Bước tuỳ chọn. Mặc định không làm gì và trả về None.
        Override nếu cần chuẩn bị dữ liệu đầu vào cho crawl.
        """
        return None

    @abstractmethod
    def crawl(self, extracted_data: Any = None) -> Any:
        """
        Bước bắt buộc. Thu thập dữ liệu, có thể nhận extracted_data từ extract().
        """
        raise NotImplementedError("crawl method must be implemented")

    def transform(self, data: Any, transformation_steps: list[Any] | None = None) -> Any:
        """
        Bước tuỳ chọn. Nếu có transformation_steps thì áp dụng tuần tự,
        ngược lại trả về nguyên dữ liệu đầu vào.
        """
        if transformation_steps:
            for step in transformation_steps:
                data = step.transform(data)
        return data

    @abstractmethod
    def load(self, data: Any) -> None:
        """
        Bước bắt buộc. Ghi dữ liệu đã crawl/transform ra đích.
        """
        raise NotImplementedError("load method must be implemented")

    def run(self, transformation_steps: list[Any] | None = None) -> Any:
        extracted_data = self.extract()
        crawled_data = self.crawl(extracted_data)
        transformed_data = self.transform(crawled_data, transformation_steps)
        self.load(transformed_data)
        return transformed_data