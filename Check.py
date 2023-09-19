from pathlib import Path

file_path = "/path/to/your/file.txt"  # Replace with the actual file path

if Path(file_path).exists():
    print(f"The file '{file_path}' exists.")
else:
    print(f"The file '{file_path}' does not exist.")
