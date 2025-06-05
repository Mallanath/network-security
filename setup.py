'''
The setup.py file is an essential part of packaging and 
distributing Python projects. It is used by setuptools 
(or distutils in older Python versions) to define the configuration 
of your project, such as its metadata, dependencies, and more
'''

from setuptools import setup, find_packages
from typing import List 

def get_requirements() -> List[str]:
    """
    This function reads the requirements.txt file and returns the list of requirements.
    """

    requirement_lst:List[str] = []
    try:
        with open('requirements.txt', 'r') as file:
            #Read lines from the file
            lines=file.readlines()
            ## process each line
            for line in lines:
                ## remove leading/trailing whitespaces
                requirement = line.strip()
                ## ignore empty lines and -e . 
                if requirement and requirement != "-e .":
                    requirement_lst.append(requirement)
    except FileNotFoundError:
        print("Requirements.txt file not found")

    return requirement_lst


setup(
    name="NetworkSecurity",
    version="0.0.1",
    author="Mallanat Mise",
    author_email="mallanathmise@gmail.com",
    packages=find_packages(),
    install_requires=get_requirements()
)
              