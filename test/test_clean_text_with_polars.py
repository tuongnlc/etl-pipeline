import polars as pl


text_ = """
Bộ Tài chính đang lấy ý kiến dự thảo nghị định cho phép doanh nghiệp làm dự án PPP phát hành trái phiếu ra công chúng theo cơ chế riêng, nhằm mở thêm kênh huy động vốn cho cao tốc, metro, đường sắt và nhiều công trình hạ tầng lớn trong giai đoạn tới.

Theo hồ sơ dự thảo, cơ quan quản lý cho rằng nhu cầu đầu tư hạ tầng của Việt Nam đang tăng rất mạnh trong khi khả năng cung cấp vốn dài hạn của hệ thống ngân hàng ngày càng hạn chế. Giai đoạn 2025-2040, nhu cầu vốn cho hạ tầng ước tính khoảng 30-40 tỷ USD mỗi năm, trải rộng từ giao thông, năng lượng đến hạ tầng số.

Trong bối cảnh đó, mô hình hợp tác công - tư (PPP) được xem là hướng đi quan trọng để kéo thêm nguồn lực tư nhân vào các dự án lớn. Tuy nhiên, Bộ Tài chính đánh giá doanh nghiệp PPP hiện vẫn khó tiếp cận kênh trái phiếu do nhiều quy định được thiết kế cho doanh nghiệp truyền thống thay vì doanh nghiệp dự án hạ tầng.

Theo cơ quan soạn thảo, phần lớn doanh nghiệp PPP chỉ được thành lập để triển khai một dự án duy nhất, thời gian hoàn vốn thường kéo dài hàng chục năm. Giai đoạn đầu xây dựng và vận hành, doanh thu chưa cao nhưng áp lực chi phí vốn lại lớn do tỷ lệ vay nợ cao. Điều này khiến nhiều đơn vị khó đáp ứng các điều kiện phát hành trái phiếu hiện hành như lịch sử hoạt động, kết quả kinh doanh hay năng lực tài chính.

Đây cũng là lý do Bộ Tài chính đề xuất xây dựng cơ chế riêng cho trái phiếu PPP, thay vì áp dụng hoàn toàn cơ chế phát hành của doanh nghiệp thông thường.

Theo dự thảo, doanh nghiệp dự án PPP có thể được nới một số điều kiện phát hành nếu có hợp đồng dự án hợp pháp, phương án tài chính rõ ràng và cơ chế bảo đảm thanh toán trái phiếu. Đổi lại, siết mạnh phần quản trị rủi ro. Toàn bộ trái phiếu PPP phát hành ra công chúng phải là trái phiếu có bảo đảm, không chuyển đổi và không kèm chứng quyền.

Với dự án chưa đi vào khai thác, trái phiếu phải được ngân hàng hoặc tổ chức tài chính bảo lãnh thanh toán toàn bộ gốc và lãi. Khi dự án đã vận hành, doanh nghiệp có thể dùng quyền thu phí, quyền kinh doanh công trình hoặc tài sản hạ tầng để làm tài sản bảo đảm.

Một điểm đáng chú ý khác là dự thảo cho phép dùng chính quyền khai thác công trình hạ tầng làm tài sản bảo đảm cho trái phiếu. Điều này cho thấy cơ quan quản lý đang muốn tiếp cận theo mô hình “project finance (tài trợ dự án)” phổ biến ở nhiều nước, tức đánh giá dự án dựa trên dòng tiền tương lai thay vì chỉ nhìn vào báo cáo tài chính hiện tại.

Bộ Tài chính cũng yêu cầu trái phiếu PPP phải được xếp hạng tín nhiệm độc lập, hoặc tổ chức bảo lãnh thanh toán phải có xếp hạng tín nhiệm. Doanh nghiệp phát hành phải công bố định kỳ tiến độ dự án, tình hình sử dụng vốn và các thông tin ảnh hưởng khả năng trả nợ.

Nếu xuất hiện sự cố liên quan tới tài sản bảo đảm hoặc dòng tiền dự án, đơn vị phát hành phải công bố thông tin trong vòng 24 giờ.

Phần quản lý dòng tiền sau phát hành cũng được thiết kế khá chặt. Tiền huy động chỉ được dùng cho dự án PPP hoặc cơ cấu nợ của dự án. Trong thời gian chưa giải ngân hết, doanh nghiệp chỉ được gửi ngân hàng, mua trái phiếu Chính phủ hoặc chứng chỉ tiền gửi, không được cho vay hay dùng làm tài sản bảo đảm cho nghĩa vụ của bên thứ ba.

Các nước kéo vốn cho hạ tầng bằng trái phiếu thế nào?

Bộ Tài chính dẫn kinh nghiệm từ nhiều quốc gia đã dùng thị trường vốn để kéo tiền vào các dự án hạ tầng lớn thay vì phụ thuộc chủ yếu vào ngân hàng. Để hút dòng tiền dài hạn, các nước này thường xây cơ chế riêng cho trái phiếu hạ tầng như ưu đãi thuế cho nhà đầu tư, phát triển quỹ đầu tư hạ tầng, hỗ trợ bảo lãnh tín dụng hoặc thậm chí có cơ chế bảo lãnh từ Chính phủ nhằm giảm rủi ro cho người mua trái phiếu.

Một số mô hình đã huy động được lượng vốn rất lớn cho các dự án giao thông và năng lượng. Trung Quốc từng phát hành thành công trái phiếu cho tuyến đường sắt cao tốc Bắc Kinh - Thượng Hải và nhiều dự án cao tốc quy mô lớn. Thái Lan cũng dùng trái phiếu để triển khai Hành lang kinh tế phía Đông (EEC), dự án được xem là động lực tăng trưởng mới của nước này.

Tại Ấn Độ, các quỹ tín thác đầu tư hạ tầng (InvIT) đã huy động khoảng 13.4 tỷ USD từ thị trường. Đây đang trở thành một trong những kênh vốn quan trọng cho các dự án đường bộ, logistics và năng lượng của quốc gia Nam Á.

Bộ Tài chính cũng dẫn một số dự án PPP quốc tế có tỷ lệ phát hành trái phiếu thành công cao nhờ cơ chế giảm rủi ro rõ ràng. Chẳng hạn, dự án cao tốc A11 Bruges-Knokke tại Bỉ huy động khoảng 578 triệu EUR trái phiếu với hỗ trợ bảo lãnh tín dụng từ Ngân hàng Đầu tư châu Âu (EIB). Tại Nigeria, một dự án điện PPP huy động gần 900 triệu USD nhờ có bảo lãnh thanh toán của World Bank. Trong khi đó, dự án cao tốc Indiana Toll Road tại Mỹ huy động khoảng 3.8 tỷ USD trái phiếu với tài sản bảo đảm là nguồn thu phí của dự án.

Theo Bộ Tài chính, các dự án hạ tầng quy mô lớn trên thế giới thường cần cơ chế vốn riêng, thời hạn dài và đi kèm các công cụ giảm rủi ro để thu hút nhà đầu tư tổ chức. Đây cũng là một trong những cơ sở để cơ quan quản lý đề xuất xây dựng khung phát hành trái phiếu riêng cho doanh nghiệp PPP tại Việt Nam.

"Ông lớn" BOT nhiều năm than khó

Trở ngại trong mô hình kinh doanh của dự án PPP trên thực tế đã được nhiều doanh nghiệp hạ tầng nhắc tới trong vài năm gần đây. CTCP Đầu tư Hạ tầng Kỹ thuật TPHCM (HOSE: CII) - đơn vị tham gia nhiều dự án BOT ở TPHCM - từng nhiều lần kiến nghị sửa cơ chế kế toán cho doanh nghiệp BOT vì đặc thù ngành này thường rơi vào tình trạng “lỗ giả, lời thật”.

Theo giải thích của CII, các dự án BOT thường có tổng mức đầu tư rất lớn, trong đó vốn vay chiếm khoảng 70-80%. Điều này khiến chi phí lãi vay ở giai đoạn đầu khai thác rất cao, trong khi doanh thu ban đầu chưa lớn do lưu lượng xe còn thấp.

Vì vậy, báo cáo tài chính của nhiều dự án BOT thường ghi nhận lỗ trong các năm đầu dù dòng tiền dài hạn vẫn khả quan. Ngược lại, càng về cuối vòng đời dự án, doanh thu tăng dần theo lưu lượng xe và mức phí, còn chi phí lãi vay giảm khi dư nợ thu hẹp, khiến lợi nhuận tăng nhanh hơn.

CII cho rằng cách hạch toán hiện nay phần nào chưa phản ánh đầy đủ hiệu quả tài chính dài hạn của các dự án BOT, từ đó ảnh hưởng đến khả năng huy động vốn.

Ở góc nhìn thị trường, đơn vị xếp hạng tín nhiệm Saigon Ratings cũng cho rằng Việt Nam đang bước vào chu kỳ đầu tư hạ tầng quy mô lớn, kéo theo nhu cầu vốn trung và dài hạn tăng mạnh trong nhiều năm tới.

Chỉ riêng lĩnh vực giao thông, quy hoạch đến năm 2030 đặt mục tiêu phát triển hơn 9,000km cao tốc, gần 30,000km quốc lộ và hơn 1,500km đường sắt tốc độ cao, với tổng nhu cầu vốn khoảng 2.6 triệu tỷ đồng.

Saigon Ratings đánh giá cấu trúc vốn cho hạ tầng thời gian tới có thể dịch chuyển theo hướng ngân sách Nhà nước đóng vai trò “vốn mồi”, ngân hàng cung cấp tín dụng bổ trợ, còn thị trường trái phiếu đảm nhiệm nguồn vốn trung và dài hạn.

Đơn vị này cũng nhận định khi khung pháp lý cho trái phiếu hạ tầng dần hoàn thiện từ năm 2026, trái phiếu doanh nghiệp có thể trở thành kênh huy động vốn chủ lực cho các dự án PPP quy mô lớn.

Hiện thị trường vẫn thiếu nhóm nhà đầu tư dài hạn như quỹ hưu trí, doanh nghiệp bảo hiểm hay định chế tài chính phát triển - những tổ chức phù hợp với các dự án hạ tầng có vòng đời kéo dài hàng chục năm.

Tử Kính

FILI

- 17:43 15/05/2026
"""


class CleanTextPolars:
    def transform(self, df: pl.DataFrame, col_name: str) -> pl.DataFrame:
        # Remove footer metadata from the article text.
        # Normalize tabs into single spaces.
        # Trim leading and trailing whitespace from the full text.
        # Split text into lines for line-level cleaning.
        # Preserve blank lines to keep paragraph structure.
        # Replace special characters with spaces while keeping Unicode letters/digits.
        # Collapse repeated spaces and tabs into a single space.
        # Trim whitespace around each cleaned line.
        # Join cleaned lines back into a single multiline string.
        footer_pattern = r"[^\n]+\n+FILI\n+- \d{2}:\d{2} \d{2}/\d{2}/\d{4}"

        return df.with_columns(
            pl.col(col_name)
            .fill_null("")
            .str.replace(footer_pattern, "", literal=False)
            .str.replace_all(r"\t+", " ", literal=False)
            .str.strip_chars()
            .str.split("\n")
            .list.eval(
                pl.when(pl.element().str.strip_chars() == "")
                .then(pl.lit(""))
                .otherwise(
                    pl.element()
                    .str.replace_all(r"[^\p{L}\p{N}_ \t]", " ", literal=False)
                    .str.replace_all(r"[ \t]+", " ", literal=False)
                    .str.strip_chars()
                )
            )
            .list.join("\n")
            .alias(col_name)
        )


df = pl.DataFrame({"text": [text_]})
df = CleanTextPolars().transform(df, "text")
print(df.item(0, "text"))
