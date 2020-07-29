import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="processmanager",
    version="0.1.1",
    author="Brandon M. Pace",
    author_email="brandonmpace@gmail.com",
    description="A multiprocessing process manager for Python programs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="multiprocessing multicore process offload manager",
    license="GNU Lesser General Public License v3 or later",
    install_requires=["freezehelper>=2.0.0", "logcontrol>=0.2.1"],
    platforms=["any"],
    python_requires=">=3.6.5",
    project_urls={
        "Code and Issues": "https://github.com/brandonmpace/processmanager",
        "Documentation": "https://processmanager.readthedocs.io"
    },
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3"
    ]
)
