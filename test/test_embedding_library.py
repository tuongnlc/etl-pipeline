# import os
# import numpy as np
# from google import genai
# from google.genai import types  # Import types để định nghĩa rõ ràng cấu trúc dữ liệu

# # Khởi tạo client với API key của bạn
# client = genai.Client(api_key="AQ.Ab8RN6Jsbqvkq4R1Q7j9CK3JUyDR40dCj0mQajaJIzfOM5dMgA")

# # Khuyên dùng model chuẩn để kiểm soát dimension (ví dụ 768 hoặc 1536)
# MODEL_ID = "gemini-embedding-2" 

# texts_to_embed = [
#     "Dữ liệu giao dịch khớp lệnh sàn HOSE" * 10,
#     "Tín hiệu dòng tiền lớn mua gom cổ phiếu" * 10,
#     "Xu hướng chỉ số VN-Index trong ngắn hạn" * 10,
#     "Phân tích báo cáo tài chính doanh nghiệp" * 10
# ]

# from google.genai import types

# formal_contents = [types.Content(parts=[types.Part.from_text(text=t)]) for t in texts_to_embed]

# # multiple contents with config
# response = client.models.embed_content( 
#     # model='gemini-embedding-001', #output shape is (4, 10)
#     model='gemini-embedding-2', #output shape is (1, 10)
#     contents=formal_contents,
#     config=types.EmbedContentConfig(output_dimensionality=10),
# )

# # print(response)

# vectors = [emb.values for emb in response.embeddings]

# # Chuyển thành mảng numpy để kiểm tra shape
# import numpy as np
# matrix = np.array(vectors)

# print(matrix.shape)