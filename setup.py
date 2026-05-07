from setuptools import setup, find_packages

setup(
    name="multi-tenant-onboarding",
    version="0.1.0",
    packages=find_packages(exclude=["tests*", "notebooks*"]),
    install_requires=[
        "openai>=1.0",
        "pyyaml>=6.0",
        "jinja2>=3.1",
        "great-expectations>=0.18",
    ],
    entry_points={
        "console_scripts": [
            "orchestrator=orchestrator:run",
            "deviation-detector=agents.deviation_detector:run",
            "anomaly-detector=agents.anomaly_detector:run",
            "changelog-summariser=agents.changelog_summariser:run",
        ]
    },
    python_requires=">=3.10",
)
