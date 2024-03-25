from setuptools import setup, find_packages

setup(
    name='ssdeep_databricks',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        # Add your project dependencies here, e.g.,
        # 'numpy',
    ],
    extras_require={
        'dev': [
            # Add your development dependencies here, e.g.,
            # 'pytest',
        ]
    },
    entry_points={
        # Add any command-line entry points, if necessary
    }
)



