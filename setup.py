import codecs
import os.path
from setuptools import find_packages, setup

project = 'pirec2'
here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

about = {}
with open(os.path.join(here, project, "__version__.py")) as f:
    exec(f.read(), about)


setup(
    name=project,
    version=about['__version__'],
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=True,
    author='Jon Stutters',
    author_email='j.stutters@ucl.ac.uk',
    description='A Make-like system designed for neuroimaging analysis',
    long_description=long_description,
    url='https://github.com/jstutters/pirec2',
    install_requires=[],
    setup_requires=[],
    tests_require=[],
    entry_points={
        'console_scripts': ['pirec2=pirec2.cli:main']
    },
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Topic :: System :: Logging'
    ]
)
