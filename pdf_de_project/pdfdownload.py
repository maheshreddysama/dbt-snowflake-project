import requests
import os

# Create the directory if it doesn't exist
os.makedirs("data/raw", exist_ok=True)

url = "https://www.w3.org/WAI/WCAG21/wcag21.pdf"
print(f"Downloading from {url}...")

r = requests.get(url)

# Save the file
with open("data/raw/sample_wcag.pdf", "wb") as f:
    f.write(r.content)

print("Downloaded sample PDF to data/raw/sample_wcag.pdf")