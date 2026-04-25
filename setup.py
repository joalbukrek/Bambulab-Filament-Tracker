from setuptools import find_packages, setup


setup(
    name="bambulab-filament-tracker",
    version="0.1.0",
    description="Local Bambu Lab A1/AMS Lite filament usage tracker",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=["paho-mqtt>=1.6,<3"],
    entry_points={
        "console_scripts": [
            "bambu-track=bambulab_filament_tracker.cli:main",
        ],
    },
    python_requires=">=3.9",
)
