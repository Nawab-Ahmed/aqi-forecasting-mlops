from src.storage.raw_repository import RawMergedDataRepository
repo = RawMergedDataRepository()
print("Total records:", repo.collection.count_documents({}))
repo.close()
