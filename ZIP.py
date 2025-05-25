import zipfile
from datetime import datetime

# Set a fixed or null date
zero_date = (1980, 1, 1, 0, 0, 0)  # Minimum allowed date in zip format

with zipfile.ZipFile('original.zip', 'r') as src_zip:
    with zipfile.ZipFile('cleaned.zip', 'w') as new_zip:
        for item in src_zip.infolist():
            data = src_zip.read(item.filename)
            item.date_time = zero_date
            item.create_system = 0  # Prevent platform-specific attributes
            new_zip.writestr(item, data)