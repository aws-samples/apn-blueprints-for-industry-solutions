import setuptools
 
with open("readme.md", "r") as fh:
    long_description = fh.read()
 
setuptools.setup(
    name="c360simulator",
    version="0.0.1",
    author="Shishir Choudhary",
    author_email="ishishir@amazon.com",
    description="C360 Simualator",
    long_description="",
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    include_package_data=True,
    package_data={
        'c360simulator': ['**/*.csv',"**/*.yaml"]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)