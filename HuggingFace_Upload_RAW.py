from huggingface_hub import HfApi
import os

api = HfApi()
api.upload_large_folder(
    folder_path="./Dataset",
    repo_id="DinoPonjevic/NYC_TaxiData_RAW",
    repo_type="dataset",
)
