import setuptools

setuptools.setup(
    name="namespace-onboarding",
    version="1.0.0",
    description="Namespace onboarding migration job - assigns namespace permissions based on resource usage",
    packages=setuptools.find_packages(),
    install_requires=[
        "psycopg2-binary>=2.9.0",
        "pyhocon>=0.3.59",
    ],
    python_requires=">=3.12",
)
