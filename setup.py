from setuptools import setup, find_packages

setup(
    name="tesseract-compression",
    version="1.0.0",
    description="Tesseract Compression System - Deduplication-based archiver for cold storage",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "tqdm>=4.60.0",
        "cryptography>=41.0.0",
        "blake3>=0.3.0",
    ],
    extras_require={
        "dev": ["pytest>=7.0", "pytest-cov>=4.0"],
    },
    entry_points={
        "console_scripts": [
            "tesseract=tesseract.cli:main",
        ],
    },
)
