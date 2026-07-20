# from etl_pipeline.infrastructure.polars.etl.extract.qdrant_extractor import QdrantExtractorWithPayloadFilter
# # from etl_pipeline.templates.etl.load.qdrant_loader import QdrantLoader
# from etl_pipeline.infrastructure.polars.etl.load.qdrant_loader import QdrantLoader


# qdrant_extractor = QdrantExtractorWithPayloadFilter(
#     qdrant_url="http://localhost:6333",
#     # qdrant_collection_name="stock_price_embedded",
#     collection_name="stock_price_embedded",
#     payload_filter={"is_embedded": 1},
#     # with_payload=True,
#     with_vectors=True,
# )

# df_ = qdrant_extractor.extract()
# print(len(df_))
# print(df_.columns)

# df_ = df_.rename(
#     {
#         "gemini_dense_vector": "dense_vector_embedded",
#         "bm25_sparse_indices": "sparse_vector_indices",
#         "bm25_sparse_values": "sparse_vector_value",
#     }
# )
# #df_renamed = df.rename({"old_name": "new_name", "another_old": "cool_name"})

# qdrant_loader = QdrantLoader(
#     qdrant_url="http://localhost:6333",
#     destination_collection_name="financial_data_embedded",
# )

# qdrant_loader.load(
#     df_,
#     dense_vector_column="dense_vector_embedded", 
#     sparse_vector_indices_column="sparse_vector_indices", 
#     sparse_vector_values_column="sparse_vector_value"
# )

# from qdrant_client import QdrantClient

# client = QdrantClient(url="http://localhost:6333", timeout=300)

source_name = "stock_price_embedded"
# target_name = "financial_data_embedded"

# # # # 1. Take a snapshot of your source collection
# # # print("Creating snapshot...")
# snapshot_info = client.create_snapshot(collection_name=source_name)

# # 2. Recover the snapshot into a completely new collection name
# print(f"Restoring snapshot into '{target_name}'...")
# client.recover_snapshot(
#     collection_name=target_name,
#     location=f"http://localhost:6333/collections/{source_name}/snapshots/{snapshot_info.name}",
#     wait=False
# )

# # 3. Clean up the temporary snapshot file if you want
# client.delete_snapshot(collection_name=source_name, snapshot_name=snapshot_info.name)
# print("Done!")


from qdrant_client import QdrantClient

# # Thêm timeout lớn để không bị lỗi nữa
client = QdrantClient(url="http://localhost:6333", timeout=300.0)

# 1. Xem danh sách các snapshot đang có của collection này
snapshots = client.list_snapshots(collection_name=source_name)

# 2. Xóa sạch các snapshot cũ để giải phóng ổ cứng
for snap in snapshots:
    print(f"Đang xóa snapshot: {snap.name}")
    client.delete_snapshot(collection_name=source_name, snapshot_name=snap.name)

print("Đã dọn dẹp xong!")