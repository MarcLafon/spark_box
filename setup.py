from setuptools import setup



setup(name='toolbox',
    version='0.0.1',
    description='Toolbox Spark',
    long_description=open('README.md').read(),
    url='',
    author='quinten',
    author_email='',
    include_package_data=True,
    packages=['toolbox','toolbox.dataprep','toolbox.ml','toolbox.cleaning'],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python",
        "Natural Language :: French",
        "Programming Language :: Python :: 2.7",
        ]
    )
    
