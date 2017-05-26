from setuptools import setup



setup(name='toolbox',
    version='0.0.1',
    description='Toolbox Quinten',
    long_description=open('README.md').read(),
    url='https://master.quinten.local/gitlab/quinten/toolbox.git',
    author='quinten',
    author_email='m.lafon@quinten-france.com',
    include_package_data=True,
    packages=['toolbox','toolbox.dataprep','toolbox.ml','toolbox.cleaning'],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python",
        "Natural Language :: French",
        "Programming Language :: Python :: 2.7",
        ]
    )
    
