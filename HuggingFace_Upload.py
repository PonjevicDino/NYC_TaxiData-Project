from huggingface_hub import HfApi
import os

api = HfApi()
api.upload_large_folder(
    folder_path="./Dataset_Modified",
    repo_id="DinoPonjevic/NYC_TaxiData",
    repo_type="dataset",
)
